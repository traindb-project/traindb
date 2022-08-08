/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import net.hydromatic.quidem.Quidem.ConnectionFactory;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.Description;
import traindb.common.TrainDBLogger;

import org.junit.rules.TestWatcher;

public class JdbcClientTest {
  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(JdbcClientTest.class);
  private static final String TEST_DBNAME = "myschema";
  private static ConnectionFactory connFactory;

  @Rule
  public TestWatcher watchman = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      LOG.info("start JUnit test: " + description.getDisplayName());
    }

    @Override
    protected void finished(Description description) {
      LOG.info("finished. JUnit test: " + description.getDisplayName());
    }
  };

  @BeforeAll
  static void initAll() throws Exception {
    Class.forName("traindb.jdbc.Driver");
    connFactory = TestUtil.createConnectionFactory();
//    setupTestData();
  }

  @AfterAll
  static void tearDownAll() throws Exception {
  }

  private static Connection getConnection(String name) {
    try {
      Connection conn = connFactory.connect(name, false);
      LOG.debug("GetConnection: " + name + "\n");
      return conn;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  // Use this directly to source DBMS
  private static void setupTestData() throws SQLException {
    Connection conn = getConnection(TEST_DBNAME);
    Statement stmt = conn.createStatement();

    stmt.execute("DROP SCHEMA IF EXISTS myschema");
    stmt.execute("CREATE SCHEMA myschema");
    stmt.execute("CREATE TABLE myschema.sales (" +
        "  product     varchar(100)," +
        "  price       double," +
        "  productid   int)");

    stmt.execute("CREATE TABLE myschema.products (" +
        "  name        varchar(100)," +
        "  productid   int)");

    //int count = 100000;
    int count = 1000;
    List<String> productList = Arrays.asList("milk", "egg", "juice");
    for (int i = 0; i < count; i++) {
      int randInt = ThreadLocalRandom.current().nextInt(0, 3);
      String product = productList.get(randInt);
      double price = (randInt + 2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
      if (i < 10) {
        price = 9;
      }
      stmt.execute(String.format(
          "INSERT INTO myschema.sales (product, price, productid) VALUES('%s', %.0f, %d)",
          product, price, randInt));
      stmt.execute(String.format(
          "INSERT INTO myschema.products (name, productid) VALUES('%s', %d)",
          product, randInt));
    }

    stmt.close();
    conn.close();
  }

  @Test
  public void testJdbcClientExecuteQuery() throws SQLException {
    Connection conn = getConnection(TEST_DBNAME);
    Statement vstmt = conn.createStatement();

    String sumQuery = "SELECT SUM(price), AVG(price) FROM myschema.sales WHERE product='egg'";

    ResultSet rs = vstmt.executeQuery(sumQuery);
    TestUtil.printResultSet(rs);
  }

  @Test
  public void testJdbcClientPrepareStmtQuery() throws SQLException {
    Connection conn = getConnection(TEST_DBNAME);
    String sql =
        "SELECT COUNT(*) FROM myschema.sales JOIN myschema.products " +
        "ON myschema.sales.productid = myschema.products.productid " +
        "WHERE sales.price < 10 " +
        "GROUP BY products.name " +
        "ORDER BY COUNT(*) DESC";

    PreparedStatement pstmt = conn.prepareStatement(sql);
    ResultSet rs = pstmt.executeQuery();
    TestUtil.printResultSet(rs);
  }

  @Test
  public void testJdbcClientPrepareStmtQuery2() throws SQLException {
    Connection conn = getConnection(TEST_DBNAME);
    String sql =
        "SELECT COUNT(*) FROM " +
        "(SELECT price, productid FROM myschema.sales) AS t INNER JOIN myschema.products " +
        "ON t.productid = products.productid " +
        "WHERE t.price < 10 " +
        "GROUP BY products.name " +
        "ORDER BY COUNT(*) IS NULL DESC, COUNT(*) DESC";

    PreparedStatement pstmt = conn.prepareStatement(sql);
    ResultSet rs = pstmt.executeQuery();
    TestUtil.printResultSet(rs);
  }

  @Test
  public void testJdbcClientPrepareStmtQuery3() throws SQLException {
    Connection conn = getConnection(TEST_DBNAME);
    String sql =
        "SELECT t2.EXPR$0 FROM " +
          "(SELECT products.name, COUNT(*) AS EXPR$0 FROM " +
            "(SELECT price, productid FROM myschema.sales WHERE price < 10) AS t0 " +
          "INNER JOIN myschema.products ON t0.productid = products.productid " +
          "GROUP BY products.name " +
          "ORDER BY COUNT(*) IS NULL DESC, COUNT(*) DESC) AS t2";

    PreparedStatement pstmt = conn.prepareStatement(sql);
    ResultSet rs = pstmt.executeQuery();
    TestUtil.printResultSet(rs);
  }

  @Test
  public void testJdbcClientTrainModel() throws SQLException {
    Connection conn = getConnection(TEST_DBNAME);
    Statement stmt = conn.createStatement();
    TestUtil.executeIgnore(stmt, "DROP SYNOPSIS sales_syn");
    TestUtil.executeIgnore(stmt, "DROP MODEL INSTANCE tgan");
    TestUtil.executeIgnore(stmt, "DROP MODELTYPE tablegan");

    stmt.execute("CREATE MODELTYPE tablegan FOR SYNOPSIS AS LOCAL CLASS 'TableGAN' in 'models/TableGAN.py'");
    stmt.execute("TRAIN MODEL tgan MODELTYPE tablegan ON myschema.sales(product, price, productid)");
    stmt.execute("CREATE SYNOPSIS sales_syn FROM MODEL tgan LIMIT 1000");

    ResultSet rs = stmt.executeQuery("SELECT count(*) FROM myschema.sales_syn");
    TestUtil.printResultSet(rs);

    stmt.execute("DROP SYNOPSIS sales_syn");
    stmt.execute("DROP MODEL tgan");
    stmt.execute("DROP MODELTYPE tablegan");

    rs.close();
    stmt.close();
    conn.close();
  }
}
