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

package traindb;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.ConcurrentJdbcConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.metastore.CachedScrambleMetaStore;
import org.verdictdb.metastore.ScrambleMetaStore;
import org.verdictdb.metastore.VerdictMetaStore;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntaxList;
import traindb.catalog.CatalogException;
import traindb.catalog.CatalogStore;
import traindb.catalog.JDOCatalogStore;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.engine.TrainDBExecContext;
import traindb.schema.SchemaManager;


public class TrainDBContext {

  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBContext.class);
  private final String contextId;
  private DbmsConnection conn;
  private boolean isClosed = false;
  private CatalogStore catalogStore;
  private BasicDataSource dataSource;
  private SchemaManager schemaManager;
  private long executionSerialNumber = 0;
  private TrainDBConfiguration conf;
  /**
   * Maintains the list of open executions. Each query is processed on a separate execution context.
   */
  private List<TrainDBExecContext> exCtxs = new LinkedList<>();

  public TrainDBContext(BasicDataSource dataSource, Properties info) throws TrainDBException {
    String jdbcConnStr = dataSource.getUrl();
    DbmsConnection conn;
    try {
      if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnStr) instanceof MysqlSyntax) {
        conn = JdbcConnection.create(jdbcConnStr, info);
      } else {
        conn = ConcurrentJdbcConnection.create(jdbcConnStr, info);
      }
    } catch (VerdictDBException e) {
      throw new TrainDBException(e.getMessage());
    }
    this.conn = new CachedDbmsConnection(conn);
    this.contextId = RandomStringUtils.randomAlphanumeric(5);
    this.conf = new TrainDBConfiguration(info);
    this.catalogStore = new JDOCatalogStore();
    conf.loadConfiguration();
    catalogStore.start(conf.getProps());

    this.dataSource = dataSource;
    this.schemaManager = SchemaManager.getInstance(catalogStore);
    schemaManager.loadDataSource(dataSource);
  }

  /**
   * Uses a connection pool.
   *
   * @param jdbcConnectionString
   * @return TrainDBContext
   * @throws SQLException
   * @throws TrainDBException
   */
  public static TrainDBContext fromConnectionString(String jdbcConnectionString)
      throws TrainDBException {
    return fromConnectionString(jdbcConnectionString, new Properties());
  }

  /**
   * Uses a connection pool.
   *
   * @param jdbcConnectionString
   * @param info
   * @return TrainDBContext
   * @throws SQLException
   * @throws TrainDBException
   */
  public static TrainDBContext fromConnectionString(String jdbcConnectionString, Properties info)
      throws TrainDBException {
    String jdbcConnStr = removeTrainDBKeywordIfExists(jdbcConnectionString);
    String jdbcDriverClassName = getJdbcDriverClassName(jdbcConnStr);
    if (jdbcDriverClassName == null) {
      throw new TrainDBException(
          String.format("JDBC driver not found for the connection string: %s", jdbcConnStr));
    }

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(jdbcConnStr);
    dataSource.setDriverClassName(jdbcDriverClassName);
    dataSource.setValidationQuery("SELECT 1");
    if (info != null) {
      dataSource.setUsername(info.getProperty("user"));
      dataSource.setPassword(info.getProperty("password"));
    }

    return new TrainDBContext(dataSource, info);
  }

  /**
   * Uses a connection pool.
   *
   * @param jdbcConnectionString
   * @param user
   * @param password
   * @return TrainDBContext
   * @throws TrainDBException
   */
  public static TrainDBContext fromConnectionString(
      String jdbcConnectionString, String user, String password) throws TrainDBException {
    Properties info = new Properties();
    info.setProperty("user", user);
    info.setProperty("password", password);
    return fromConnectionString(jdbcConnectionString, info);
  }

  private static String removeTrainDBKeywordIfExists(String connectionString) {
    String[] tokens = connectionString.split(":");
    if (tokens[1].equalsIgnoreCase("traindb")) {
      StringBuilder newConnectionString = new StringBuilder();
      for (int i = 0; i < tokens.length; i++) {
        if (i != 1) {
          newConnectionString.append(tokens[i]);
        }
      }
      connectionString = newConnectionString.toString();
    } else {
      // do nothing
    }
    return connectionString;
  }

  private static String getJdbcDriverClassName(String jdbcConnectionString) {
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString);
    if (syntax == null) {
      return null;
    }
    Collection<String> driverClassNames = syntax.getCandidateJDBCDriverClassNames();
    for (String className : driverClassNames) {
      try {
        Class.forName(className);
        LOG.debug(className + " has been loaded into the classpath.");
        return className;
      } catch (ClassNotFoundException e) {
        /* do nothing */
      }
    }
    return null;
  }

  public DbmsConnection getConnection() {
    return conn;
  }

  public void setDefaultSchema(String schema) {
    try {
      conn.setDefaultSchema(schema);
    } catch (VerdictDBDbmsException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    this.abort(); // terminates all ExecutionContexts first.
    conn.close();
    catalogStore.stop();
    isClosed = true;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public String getContextId() {
    return contextId;
  }

  public TrainDBConfiguration getConfiguration() {
    return conf;
  }

  public TrainDBExecContext createTrainDBExecContext() {
    long execSerialNumber = getNextExecutionSerialNumber();
    TrainDBExecContext exCtx = null;
    exCtx = new TrainDBExecContext(
        conn, catalogStore, schemaManager, contextId, execSerialNumber, conf);
    exCtxs.add(exCtx);
    return exCtx;
  }

  private synchronized long getNextExecutionSerialNumber() {
    executionSerialNumber++;
    return executionSerialNumber;
  }

  public void removeTrainDBExecContext(TrainDBExecContext exCtx) {
    exCtxs.remove(exCtx);
  }

  public void abort() {
    for (TrainDBExecContext exCtx : exCtxs) {
      exCtx.terminate();
    }
  }

  public VerdictSingleResult sql(String query) throws TrainDBException {
    TrainDBExecContext exCtx = createTrainDBExecContext();
    VerdictSingleResult result = exCtx.sql(query, false);
    removeTrainDBExecContext(exCtx);
    return result;
  }
}
