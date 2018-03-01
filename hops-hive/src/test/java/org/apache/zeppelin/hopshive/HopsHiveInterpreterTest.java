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

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.completer.CompletionType;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.FIFOScheduler;
import org.apache.zeppelin.scheduler.ParallelScheduler;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.Before;
import org.junit.Test;

import com.mockrunner.jdbc.BasicJDBCTestCaseAdapter;

/**
 * JDBC interpreter unit tests
 */
public class HopsHiveInterpreterTest extends BasicJDBCTestCaseAdapter {

  static String hiveServer = "localhost:9085";
  static String cert_pwd = "adminpw";
  static String jdbcConnection;
  InterpreterContext interpreterContext;

  private static String getJdbcConnection() {
    return "jdbc:hive2://" + hiveServer + "/default;auth=noSasl;ssl=true;twoWay=true;" +
        "sslTrustStore=/home/fabio/Uni/EIT/Second/thesis/certs/vagrant__tstore.jks;" +
        "trustStorePassword=adminpw;"+
        "sslKeyStore=/home/fabio/Uni/EIT/Second/thesis/certs/vagrant__kstore.jks;" +
        "keyStorePassword=adminpw";
  }

  public static Properties getJDBCTestProperties() {
    Properties p = new Properties();
    p.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    p.setProperty("default.url", getJdbcConnection());
    p.setProperty("common.max_count", "1000");

    return p;
  }

  @Before
  public void setUp() throws Exception {
    Class.forName("org.apache.hive.jdbc.HiveDriver");

    Connection connection = DriverManager.getConnection(getJdbcConnection());
    Statement statement = connection.createStatement();
    statement.execute("DROP TABLE IF EXISTS test_table");
    statement.execute("CREATE TABLE test_table(id varchar(255), name varchar(255))");

    PreparedStatement insertStatement = connection.prepareStatement("insert into test_table(id, name) values ('a', 'a_name'),('b', 'b_name'),('c', ?)");
    insertStatement.setObject(1, null);
    insertStatement.execute();
    interpreterContext = new InterpreterContext("", "1", null, "", "", new AuthenticationInfo("vagrant"), null, null, null, null,
        null, null);
  }


  @Test
  public void testForParsePropertyKey() throws IOException {
    HopsHiveInterpreter t = new HopsHiveInterpreter(new Properties());

    assertEquals(t.getPropertyKey("(fake) select max(cant) from test_table where id >= 2452640"),
        "fake");

    assertEquals(t.getPropertyKey("() select max(cant) from test_table where id >= 2452640"),
        "");

    assertEquals(t.getPropertyKey(")fake( select max(cant) from test_table where id >= 2452640"),
        "default");

    // when you use a %jdbc(prefix1), prefix1 is the propertyKey as form part of the cmd string
    assertEquals(t.getPropertyKey("(prefix1)\n select max(cant) from test_table where id >= 2452640"),
        "prefix1");

    assertEquals(t.getPropertyKey("(prefix2) select max(cant) from test_table where id >= 2452640"),
            "prefix2");

    // when you use a %jdbc, prefix is the default
    assertEquals(t.getPropertyKey("select max(cant) from test_table where id >= 2452640"),
            "default");
  }

  @Test
  public void testForMapPrefix() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", getJdbcConnection());
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "(fake) select * from test_table";

    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

    // if prefix not found return ERROR and Prefix not found.
    assertEquals(InterpreterResult.Code.ERROR, interpreterResult.code());
    assertEquals("Prefix not found.", interpreterResult.message().get(0).getData());
  }

  @Test
  public void testDefaultProperties() throws SQLException {
    HopsHiveInterpreter jdbcInterpreter = new HopsHiveInterpreter(getJDBCTestProperties());

    assertEquals("org.apache.hive.jdbc.HiveDriver", jdbcInterpreter.getProperty("default.driver"));
    assertEquals("jdbc:hive2://" + hiveServer + "/", jdbcInterpreter.getProperty("default.url"));
    assertEquals("1000", jdbcInterpreter.getProperty("common.max_count"));
  }

  @Test
  public void testSelectQuery() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs");
    properties.setProperty("default.certPwd", "adminpw");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "select * from test_table WHERE ID in ('a', 'b')";

    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("test_table.id\ttest_table.name\na\ta_name\nb\tb_name\n", interpreterResult.message().get(0).getData());
  }

  @Test
  public void testSplitSqlQuery() throws SQLException, IOException {
    String sqlQuery = "insert into test_table(id, name) values ('a', ';\"');" +
        "select * from test_table;" +
        "select * from test_table WHERE ID = \";'\";" +
        "select * from test_table WHERE ID = ';';" +
        "select '\n', ';';" +
        "select replace('A\\;B', '\\', 'text');" +
        "select '\\', ';';" +
        "select '''', ';'";

    Properties properties = new Properties();
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();
    List<String> multipleSqlArray = t.splitSqlQueries(sqlQuery);
    assertEquals(8, multipleSqlArray.size());
    assertEquals("insert into test_table(id, name) values ('a', ';\"')", multipleSqlArray.get(0));
    assertEquals("select * from test_table", multipleSqlArray.get(1));
    assertEquals("select * from test_table WHERE ID = \";'\"", multipleSqlArray.get(2));
    assertEquals("select * from test_table WHERE ID = ';'", multipleSqlArray.get(3));
    assertEquals("select '\n', ';'", multipleSqlArray.get(4));
    assertEquals("select replace('A\\;B', '\\', 'text')", multipleSqlArray.get(5));
    assertEquals("select '\\', ';'", multipleSqlArray.get(6));
    assertEquals("select '''', ';'", multipleSqlArray.get(7));
  }

  @Test
  public void testQueryWithEsсapedCharacters() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs");
    properties.setProperty("default.certPwd", "adminpw");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "select '\\n', ';'";
    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("_c0\t_c1\n\\n\t;\n", interpreterResult.message().get(0).getData());

    sqlQuery = "select replace('A\\;B', '\\', 'text')";
    interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("'Atext;B'\nAtext;B\n", interpreterResult.message().get(1).getData());

    sqlQuery = "select '\\', ';'";
    interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("'\\'\t';'\n\\\t;\n", interpreterResult.message().get(2).getData());

    sqlQuery = "select '''', ';'";
    interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("''''\t';'\n'\t;\n", interpreterResult.message().get(3).getData());

  }

  @Test
  public void testSelectQueryWithNull() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs");
    properties.setProperty("default.certPwd", "adminpw");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "select * from test_table WHERE ID = 'c'";

    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("test_table.id\ttest_table.name\nc\tnull\n", interpreterResult.message().get(0).getData());
  }


  @Test
  public void testSelectQueryMaxResult() throws SQLException, IOException {

    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs");
    properties.setProperty("default.certPwd", "adminpw");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "select * from test_table";

    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.message().get(0).getType());
    assertEquals("test_table.id\ttest_table.name\na\ta_name\n", interpreterResult.message().get(0).getData());
    assertEquals(InterpreterResult.Type.HTML, interpreterResult.message().get(1).getType());
    assertTrue(interpreterResult.message().get(1).getData().contains("alert-warning"));
  }

  @Test
  public void concurrentSettingTest() {
    Properties properties = new Properties();
    properties.setProperty("zeppelin.hopshive.concurrent.use", "true");
    properties.setProperty("zeppelin.hopshive.concurrent.max_connection", "10");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);

    assertTrue(t.isConcurrentExecution());
    assertEquals(10, t.getMaxConcurrentConnection());

    Scheduler scheduler = t.getScheduler();
    assertTrue(scheduler instanceof ParallelScheduler);

    properties.clear();
    properties.setProperty("zeppelin.hopshive.concurrent.use", "false");
    t = new HopsHiveInterpreter(properties);

    assertFalse(t.isConcurrentExecution());

    scheduler = t.getScheduler();
    assertTrue(scheduler instanceof FIFOScheduler);
  }

  @Test
  public void testAutoCompletion() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs");
    properties.setProperty("default.certPwd", "adminpw");
    HopsHiveInterpreter t= new HopsHiveInterpreter(properties);
    t.open();

    t.interpret("", interpreterContext);

    List<InterpreterCompletion> completionList = t.completion("sel", 3, interpreterContext);

    InterpreterCompletion correctCompletionKeyword = new InterpreterCompletion("select", "select", CompletionType.keyword.name());

    assertEquals(1, completionList.size());
    assertEquals(true, completionList.contains(correctCompletionKeyword));
  }

  @Test
  public void testAuth() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs/fake");
    properties.setProperty("default.certPwd", "adminpw");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "select * from test_table WHERE ID in ('a', 'b')";

    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);

    assertEquals(InterpreterResult.Code.ERROR, interpreterResult.code());
  }

  @Test
  public void testExcludingComments() throws SQLException, IOException {
    Properties properties = new Properties();
    properties.setProperty("common.max_count", "1000");
    properties.setProperty("common.max_retry", "3");
    properties.setProperty("default.driver", "org.apache.hive.jdbc.HiveDriver");
    properties.setProperty("default.url", "jdbc:hive2://" + hiveServer + "/default");
    properties.setProperty("default.certPath", "/home/fabio/second/thesis/certs");
    properties.setProperty("default.certPwd", "adminpw");
    properties.setProperty("default.splitQueries", "true");
    HopsHiveInterpreter t = new HopsHiveInterpreter(properties);
    t.open();

    String sqlQuery = "/*\n" +
        "select * from test_table\n" +
        "*/\n" +
        "-- a ; b\n" +
        "select * from test_table WHERE ID = ';--';\n";

    InterpreterResult interpreterResult = t.interpret(sqlQuery, interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(1, interpreterResult.message().size());
  }
}
