/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.hopshive;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.ResultMessages;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.jdbc.JDBCUserConfigurations;
import org.apache.zeppelin.jdbc.SqlCompleter;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;

/**
 * HopsHive JDBC interpreter for Zeppelin. This interpreter can also
 * be used to access HopsHive using X.509 Certificates
 *
 * <ul>
 * <li>{@code default.cert.path} - Path where to find the certificates.</li>
 * <li>{@code default.cert.passwd} - Password to unlock the archives.</li>
 * <li>{@code common.max.result} - Max number of SQL result to display.</li>
 * </ul>
 *
 */
public class HopsHiveInterpreter extends Interpreter{

  private Logger logger = LoggerFactory.getLogger(HopsHiveInterpreter.class);

  static final String COMMON_KEY = "common";
  static final String MAX_LINE_KEY = "max_count";
  static final int MAX_LINE_DEFAULT = 1000;

  static final String DEFAULT_KEY = "default";
  static final String URL_KEY = "url";
  static final String CERT_PATH_KEY = "certPath";
  static final String CERT_PWD_KEY = "certPwd";
  static final String COMPLETER_SCHEMA_FILTERS_KEY = "completer.schemaFilters";
  static final String COMPLETER_TTL_KEY = "completer.ttlInSeconds";
  static final String DEFAULT_COMPLETER_TTL = "120";
  static final String SPLIT_QURIES_KEY = "splitQueries";
  static final String DOT = ".";

  private static final String DEFAULT_CERT_PATH = "/srv/hops/certs-dir/transient";
  private static final String PWD_SEPARATOR = "/";

  private static final char WHITESPACE = ' ';
  private static final char NEWLINE = '\n';
  private static final char TAB = '\t';
  private static final String TABLE_MAGIC_TAG = "%table ";
  private static final String EXPLAIN_PREDICATE = "EXPLAIN ";

  static final String COMMON_MAX_LINE = COMMON_KEY + DOT + MAX_LINE_KEY;

  static final String EMPTY_COLUMN_VALUE = "";

  private final String CONCURRENT_EXECUTION_KEY = "zeppelin.hopshive.concurrent.use";
  private final String CONCURRENT_EXECUTION_COUNT = "zeppelin.hopshive.concurrent.max_connection";
  private final String DBCP_STRING = "jdbc:apache:commons:dbcp:";
  private final String DRIVER = "org.apache.hive.jdbc.HiveDriver";

  private final HashMap<String, Properties> basePropretiesMap;
  private final HashMap<String, JDBCUserConfigurations> jdbcUserConfigurationsMap;
  private final HashMap<String, SqlCompleter> sqlCompletersMap;

  private Connection connection = null;

  private int maxLineResults;

  public HopsHiveInterpreter(Properties property) {
    super(property);
    jdbcUserConfigurationsMap = new HashMap<>();
    basePropretiesMap = new HashMap<>();
    sqlCompletersMap = new HashMap<>();
    maxLineResults = MAX_LINE_DEFAULT;
  }

  public HashMap<String, Properties> getPropertiesMap() {
    return basePropretiesMap;
  }

  @Override
  public void open() {
    for (String propertyKey : property.stringPropertyNames()) {
      logger.debug("propertyKey: {}", propertyKey);
      String[] keyValue = propertyKey.split("\\.", 2);
      if (2 == keyValue.length) {
        logger.debug("key: {}, value: {}", keyValue[0], keyValue[1]);

        Properties prefixProperties;
        if (basePropretiesMap.containsKey(keyValue[0])) {
          prefixProperties = basePropretiesMap.get(keyValue[0]);
        } else {
          prefixProperties = new Properties();
          basePropretiesMap.put(keyValue[0].trim(), prefixProperties);
        }
        prefixProperties.put(keyValue[1].trim(), property.getProperty(propertyKey));
      }
    }

    Set<String> removeKeySet = new HashSet<>();
    for (String key : basePropretiesMap.keySet()) {
      if (!COMMON_KEY.equals(key)) {
        Properties properties = basePropretiesMap.get(key);
        if (!properties.containsKey(URL_KEY)) {
          logger.error("{} will be ignored. {}.{} is mandatory.",
              key, key, URL_KEY);
          removeKeySet.add(key);
        }
      }
    }

    for (String key : removeKeySet) {
      basePropretiesMap.remove(key);
    }
    logger.debug("HopsHive PropretiesMap: {}", basePropretiesMap);

    setMaxLineResults();
  }

  private String getJDBCConnectionURL(String URL, String user, String certsPwd, Properties p) {
    // Get certificate path and password - if Null set default
    String cert_path  = (String) p.get(CERT_PATH_KEY);
    if (cert_path == null) {
      cert_path = DEFAULT_CERT_PATH;
    }

    // Set cert properties
    return URL + ";auth=noSasl;ssl=true;twoWay=true" +
        ";sslTrustStore=" + Paths.get(cert_path, user + "__tstore.jks").toString() +
        ";trustStorePassword=" + certsPwd +
        ";sslKeyStore=" + Paths.get(cert_path, user + "__kstore.jks").toString() +
        ";keyStorePassword=" + certsPwd;
  }

  private void setMaxLineResults() {
    if (basePropretiesMap.containsKey(COMMON_KEY) &&
        basePropretiesMap.get(COMMON_KEY).containsKey(MAX_LINE_KEY)) {
      maxLineResults = Integer.valueOf(basePropretiesMap.get(COMMON_KEY).getProperty(MAX_LINE_KEY));
    }
  }

  private SqlCompleter createOrUpdateSqlCompleter(SqlCompleter sqlCompleter,
      final Connection connection, String propertyKey, final String buf, final int cursor) {
    String schemaFiltersKey = String.format("%s.%s", propertyKey, COMPLETER_SCHEMA_FILTERS_KEY);
    String sqlCompleterTtlKey = String.format("%s.%s", propertyKey, COMPLETER_TTL_KEY);
    final String schemaFiltersString = getProperty(schemaFiltersKey);
    int ttlInSeconds = Integer.valueOf(
        StringUtils.defaultIfEmpty(getProperty(sqlCompleterTtlKey), DEFAULT_COMPLETER_TTL)
    );
    final SqlCompleter completer;
    if (sqlCompleter == null) {
      completer = new SqlCompleter(ttlInSeconds);
    } else {
      completer = sqlCompleter;
    }
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        completer.createOrUpdateFromConnection(connection, schemaFiltersString, buf, cursor);
      }
    });

    executorService.shutdown();

    try {
      // protection to release connection
      executorService.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) { }
    return completer;
  }

  private void initStatementMap() {
    for (JDBCUserConfigurations configurations : jdbcUserConfigurationsMap.values()) {
      try {
        configurations.initStatementMap();
      } catch (Exception e) {
        logger.error("Error while closing paragraphIdStatementMap statement...", e);
      }
    }
  }

  @Override
  public void close() {
    try {
      initStatementMap();

      // Close open connection, if any
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      logger.error("Error while closing...", e);
    }
  }

  public JDBCUserConfigurations getJDBCConfiguration(String user) {
    JDBCUserConfigurations jdbcUserConfigurations =
      jdbcUserConfigurationsMap.get(user);

    if (jdbcUserConfigurations == null) {
      jdbcUserConfigurations = new JDBCUserConfigurations();
      jdbcUserConfigurationsMap.put(user, jdbcUserConfigurations);
    }

    return jdbcUserConfigurations;
  }

  private void setUserProperty(String propertyKey, InterpreterContext interpreterContext)
      throws SQLException, IOException {
    String user = interpreterContext.getAuthenticationInfo().getUser();

    JDBCUserConfigurations jdbcUserConfigurations =
      getJDBCConfiguration(user);

    jdbcUserConfigurations.setPropertyMap(propertyKey, basePropretiesMap.get(propertyKey));
  }

  public Connection getConnection(String propertyKey, InterpreterContext interpreterContext)
      throws ClassNotFoundException, SQLException, InterpreterException, IOException {

    if (connection != null && !connection.isClosed()) {
      return connection;
    }

    final String user =  interpreterContext.getAuthenticationInfo().getUser();
    // Hack. we use the String for Kerberos ticket to store the password of the jks certificates.
    final String certsPwd = interpreterContext.getAuthenticationInfo().getTicket();

    if (propertyKey == null || basePropretiesMap.get(propertyKey) == null) {
      return null;
    }

    JDBCUserConfigurations jdbcUserConfigurations = getJDBCConfiguration(user);
    setUserProperty(propertyKey, interpreterContext);

    final Properties properties = jdbcUserConfigurations.getPropertyMap(propertyKey);
    final String url = properties.getProperty(URL_KEY);

    this.connection = DriverManager.
        getConnection(getJDBCConnectionURL(url, user, certsPwd, properties));

    return this.connection;
  }


  private String getResults(ResultSet resultSet, boolean isTableType)
      throws SQLException {
    ResultSetMetaData md = resultSet.getMetaData();
    StringBuilder msg;
    if (isTableType) {
      msg = new StringBuilder(TABLE_MAGIC_TAG);
    } else {
      msg = new StringBuilder();
    }

    for (int i = 1; i < md.getColumnCount() + 1; i++) {
      if (i > 1) {
        msg.append(TAB);
      }
      msg.append(replaceReservedChars(md.getColumnName(i)));
    }
    msg.append(NEWLINE);

    int displayRowCount = 0;
    while (displayRowCount < getMaxResult() && resultSet.next()) {
      for (int i = 1; i < md.getColumnCount() + 1; i++) {
        Object resultObject;
        String resultValue;
        resultObject = resultSet.getObject(i);
        if (resultObject == null) {
          resultValue = "null";
        } else {
          resultValue = resultSet.getString(i);
        }
        msg.append(replaceReservedChars(resultValue));
        if (i != md.getColumnCount()) {
          msg.append(TAB);
        }
      }
      msg.append(NEWLINE);
      displayRowCount++;
    }
    return msg.toString();
  }

  private boolean isDDLCommand(int updatedCount, int columnCount) throws SQLException {
    return updatedCount < 0 && columnCount <= 0 ? true : false;
  }

  /*
  inspired from https://github.com/postgres/pgadmin3/blob/794527d97e2e3b01399954f3b79c8e2585b908dd/
    pgadmin/dlg/dlgProperty.cpp#L999-L1045
   */
  protected ArrayList<String> splitSqlQueries(String sql) {
    ArrayList<String> queries = new ArrayList<>();
    StringBuilder query = new StringBuilder();
    char character;

    Boolean multiLineComment = false;
    Boolean singleLineComment = false;
    Boolean quoteString = false;
    Boolean doubleQuoteString = false;

    for (int item = 0; item < sql.length(); item++) {
      character = sql.charAt(item);

      if ((singleLineComment && (character == '\n' || item == sql.length() - 1))
          || (multiLineComment && character == '/' && sql.charAt(item - 1) == '*')) {
        singleLineComment = false;
        multiLineComment = false;
        if (item == sql.length() - 1 && query.length() > 0) {
          queries.add(StringUtils.trim(query.toString()));
        }
        continue;
      }

      if (singleLineComment || multiLineComment) {
        continue;
      }

      if (character == '\'') {
        if (quoteString) {
          quoteString = false;
        } else if (!doubleQuoteString) {
          quoteString = true;
        }
      }

      if (character == '"') {
        if (doubleQuoteString && item > 0) {
          doubleQuoteString = false;
        } else if (!quoteString) {
          doubleQuoteString = true;
        }
      }

      if (!quoteString && !doubleQuoteString && !multiLineComment && !singleLineComment
          && sql.length() > item + 1) {
        if (character == '-' && sql.charAt(item + 1) == '-') {
          singleLineComment = true;
          continue;
        }

        if (character == '/' && sql.charAt(item + 1) == '*') {
          multiLineComment = true;
          continue;
        }
      }

      if (character == ';' && !quoteString && !doubleQuoteString) {
        queries.add(StringUtils.trim(query.toString()));
        query = new StringBuilder();
      } else if (item == sql.length() - 1) {
        query.append(character);
        queries.add(StringUtils.trim(query.toString()));
      } else {
        query.append(character);
      }
    }

    return queries;
  }

  public InterpreterResult executePrecode(InterpreterContext interpreterContext) {
    InterpreterResult interpreterResult = null;
    for (String propertyKey : basePropretiesMap.keySet()) {
      String precode = getProperty(String.format("%s.precode", propertyKey));
      if (StringUtils.isNotBlank(precode)) {
        interpreterResult = executeSql(propertyKey, precode, interpreterContext);
        if (interpreterResult.code() != Code.SUCCESS) {
          break;
        }
      }
    }

    return interpreterResult;
  }

  private InterpreterResult executeSql(String propertyKey, String sql,
      InterpreterContext interpreterContext) {
    Connection connection;
    Statement statement;
    ResultSet resultSet = null;
    String paragraphId = interpreterContext.getParagraphId();
    String user = interpreterContext.getAuthenticationInfo().getUser();

    boolean splitQuery = false;
    String splitQueryProperty = getProperty(String.format("%s.%s", propertyKey, SPLIT_QURIES_KEY));
    if (StringUtils.isNotBlank(splitQueryProperty) && splitQueryProperty.equalsIgnoreCase("true")) {
      splitQuery = true;
    }

    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
    try {
      connection = getConnection(propertyKey, interpreterContext);
      if (connection == null) {
        return new InterpreterResult(Code.ERROR, "Prefix not found.");
      }


      List<String> sqlArray;
      if (splitQuery) {
        sqlArray = splitSqlQueries(sql);
      } else {
        sqlArray = Arrays.asList(sql);
      }

      for (int i = 0; i < sqlArray.size(); i++) {
        String sqlToExecute = sqlArray.get(i);
        statement = connection.createStatement();

        // fetch n+1 rows in order to indicate there's more rows available (for large selects)
        statement.setFetchSize(getMaxResult());
        statement.setMaxRows(getMaxResult() + 1);

        if (statement == null) {
          return new InterpreterResult(Code.ERROR, "Prefix not found.");
        }

        try {
          getJDBCConfiguration(user).saveStatement(paragraphId, statement);

          boolean isResultSetAvailable = statement.execute(sqlToExecute);
          getJDBCConfiguration(user).setConnectionInDBDriverPoolSuccessful(propertyKey);
          if (isResultSetAvailable) {
            resultSet = statement.getResultSet();

            // Regards that the command is DDL.
            if (isDDLCommand(statement.getUpdateCount(),
                resultSet.getMetaData().getColumnCount())) {
              interpreterResult.add(InterpreterResult.Type.TEXT,
                  "Query executed successfully.");
            } else {
              String results = getResults(resultSet,
                  !containsIgnoreCase(sqlToExecute, EXPLAIN_PREDICATE));
              interpreterResult.add(results);
              if (resultSet.next()) {
                interpreterResult.add(ResultMessages.getExceedsLimitRowsMessage(getMaxResult(),
                    String.format("%s.%s", COMMON_KEY, MAX_LINE_KEY)));
              }
            }
          } else {
            // Response contains either an update count or there are no results.
            int updateCount = statement.getUpdateCount();
            interpreterResult.add(InterpreterResult.Type.TEXT,
                "Query executed successfully. Affected rows : " +
                    updateCount);
          }
        } finally {
          if (resultSet != null) {
            try {
              resultSet.close();
            } catch (SQLException e) { /*ignored*/ }
          }
          if (statement != null) {
            try {
              statement.close();
            } catch (SQLException e) { /*ignored*/ }
          }
        }
      }
      //In case user ran an insert/update/upsert statement
      if (connection != null) {
        try {
          if (!connection.getAutoCommit()) {
            connection.commit();
          }
        } catch (SQLException e) { /*ignored*/ }
      }
      getJDBCConfiguration(user).removeStatement(paragraphId);
    } catch (Throwable e) {
      logger.error("Cannot run " + sql, e);
      String errorMsg = Throwables.getStackTraceAsString(e);
      interpreterResult.add(errorMsg);
      return new InterpreterResult(Code.ERROR, interpreterResult.message());
    }
    return interpreterResult;
  }

  /**
   * For %table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(String str) {
    if (str == null) {
      return EMPTY_COLUMN_VALUE;
    }
    return str.replace(TAB, WHITESPACE).replace(NEWLINE, WHITESPACE);
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    logger.debug("Run SQL command '{}'", cmd);
    String propertyKey = getPropertyKey(cmd);

    if (null != propertyKey && !propertyKey.equals(DEFAULT_KEY)) {
      cmd = cmd.substring(propertyKey.length() + 2);
    }

    cmd = cmd.trim();
    logger.debug("PropertyKey: {}, SQL command: '{}'", propertyKey, cmd);
    return executeSql(propertyKey, cmd, contextInterpreter);
  }

  @Override
  public void cancel(InterpreterContext context) {
    logger.info("Cancel current query statement.");
    String paragraphId = context.getParagraphId();
    JDBCUserConfigurations jdbcUserConfigurations =
      getJDBCConfiguration(context.getAuthenticationInfo().getUser());
    try {
      jdbcUserConfigurations.cancelStatement(paragraphId);
    } catch (SQLException e) {
      logger.error("Error while cancelling...", e);
    }
  }

  public String getPropertyKey(String cmd) {
    boolean firstLineIndex = cmd.startsWith("(");

    if (firstLineIndex) {
      int configStartIndex = cmd.indexOf("(");
      int configLastIndex = cmd.indexOf(")");
      if (configStartIndex != -1 && configLastIndex != -1) {
        return cmd.substring(configStartIndex + 1, configLastIndex);
      } else {
        return null;
      }
    } else {
      return DEFAULT_KEY;
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    String schedulerName = HopsHiveInterpreter.class.getName() + this.hashCode();
    return isConcurrentExecution() ?
            SchedulerFactory.singleton().createOrGetParallelScheduler(schedulerName,
                getMaxConcurrentConnection())
            : SchedulerFactory.singleton().createOrGetFIFOScheduler(schedulerName);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) {
    List<InterpreterCompletion> candidates = new ArrayList<>();
    String propertyKey = getPropertyKey(buf);
    String sqlCompleterKey =
        String.format("%s.%s", interpreterContext.getAuthenticationInfo().getUser(), propertyKey);
    SqlCompleter sqlCompleter = sqlCompletersMap.get(sqlCompleterKey);

    sqlCompleter = createOrUpdateSqlCompleter(sqlCompleter, connection, propertyKey, buf, cursor);
    sqlCompletersMap.put(sqlCompleterKey, sqlCompleter);
    sqlCompleter.complete(buf, cursor, candidates);

    return candidates;
  }

  public int getMaxResult() {
    return maxLineResults;
  }

  boolean isConcurrentExecution() {
    return Boolean.valueOf(getProperty(CONCURRENT_EXECUTION_KEY));
  }

  int getMaxConcurrentConnection() {
    try {
      return Integer.valueOf(getProperty(CONCURRENT_EXECUTION_COUNT));
    } catch (Exception e) {
      return 10;
    }
  }
}
