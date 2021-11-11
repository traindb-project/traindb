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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.ConcurrentJdbcConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.CreateSchemaQuery;
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


public class TrainDBContext {

  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBContext.class);
  private final String contextId;
  private DbmsConnection conn;
  private boolean isClosed = false;
  private VerdictMetaStore metaStore;
  private CatalogStore catalogStore;
  private long executionSerialNumber = 0;
  private TrainDBConfiguration conf;
  /**
   * Maintains the list of open executions. Each query is processed on a separate execution context.
   */
  private List<TrainDBExecContext> exCtxs = new LinkedList<>();

  public TrainDBContext(DbmsConnection conn) throws TrainDBException {
    this(conn, new TrainDBConfiguration());
  }

  public TrainDBContext(DbmsConnection conn, TrainDBConfiguration conf) throws TrainDBException {
    this.conn = new CachedDbmsConnection(conn);
    this.contextId = RandomStringUtils.randomAlphanumeric(5);
    this.conf = conf;
    this.metaStore = getCachedMetaStore(conn, conf);
    this.catalogStore = new JDOCatalogStore();
    initialize(conf, catalogStore);
  }

  /**
   * This method does not support concurrent execution of queries; thus, should not be used in
   * production.
   *
   * @param jdbcConn
   * @return TrainDBContext
   * @throws TrainDBException
   */
  public static TrainDBContext fromJdbcConnection(Connection jdbcConn) throws TrainDBException {
    try {
      DbmsConnection conn = JdbcConnection.create(jdbcConn);
      return new TrainDBContext(conn);
    } catch (VerdictDBException e) {
      throw new TrainDBException(e.getMessage());
    }
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
    return fromConnectionString(jdbcConnectionString, null);
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
    if (!attemptLoadDriverClass(jdbcConnStr)) {
      throw new TrainDBException(
          String.format(
              "JDBC driver not found for the connection string: %s", jdbcConnStr));
    }
    TrainDBConfiguration conf = new TrainDBConfiguration();
    conf.parseConnectionString(jdbcConnStr);
    if (info != null) {
      conf.parseProperties(info);
    }

    try {
      if (SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnStr) instanceof MysqlSyntax) {
        return new TrainDBContext(JdbcConnection.create(jdbcConnStr, info), conf);
      } else {
        return new TrainDBContext(ConcurrentJdbcConnection.create(jdbcConnStr, info), conf);
      }
    } catch (VerdictDBException e) {
      throw new TrainDBException(e.getMessage());
    }
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

  private static boolean attemptLoadDriverClass(String jdbcConnectionString) {
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(jdbcConnectionString);
    if (syntax == null) {
      return false;
    }
    Collection<String> driverClassNames = syntax.getCandidateJDBCDriverClassNames();
    for (String className : driverClassNames) {
      try {
        Class.forName(className);
        LOG.debug(className + " has been loaded into the classpath.");
      } catch (ClassNotFoundException e) {
        /* do nothing */
      }
    }
    return true;
  }

  private VerdictMetaStore getCachedMetaStore(DbmsConnection conn, TrainDBConfiguration conf) {
    CachedScrambleMetaStore metaStore =
        new CachedScrambleMetaStore(new ScrambleMetaStore(conn, conf));
    metaStore.refreshCache();
    return metaStore;
  }

  /**
   * Creates the schema for temp tables.
   *
   * @throws TrainDBException
   */
  private void initialize(TrainDBConfiguration conf, CatalogStore catalogStore)
      throws TrainDBException {
    String schema = conf.getVerdictTempSchemaName();
    CreateSchemaQuery query = new CreateSchemaQuery(schema);
    query.setIfNotExists(true);
    try {
      conn.execute(query);
    } catch (VerdictDBException e) {
      throw new TrainDBException(e.getMessage());
    }
    conf.loadConfiguration();
    try {
      catalogStore.start(conf.getProps());
    } catch (CatalogException e) {
      throw new TrainDBException(e.getMessage());
    }
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

  public void setLoglevel(String level) {
    conf.setVerdictConsoleLogLevel(level);
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

  @Deprecated
  public JdbcConnection getJdbcConnection() {
    DbmsConnection testConn = conn;
    if (testConn instanceof CachedDbmsConnection) {
      testConn = ((CachedDbmsConnection) conn).getOriginalConnection();
    }
    return (testConn instanceof JdbcConnection) ? (JdbcConnection) testConn : null;
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
        conn, catalogStore, metaStore, contextId, execSerialNumber, conf);
    exCtxs.add(exCtx);
    return exCtx;
  }

  private synchronized long getNextExecutionSerialNumber() {
    executionSerialNumber++;
    return executionSerialNumber;
  }

  public ScrambleMetaSet getScrambleMetaSet() {
    return metaStore.retrieve();
  }

  public VerdictMetaStore getMetaStore() {
    return metaStore;
  }

  public void removeTrainDBExecContext(TrainDBExecContext exCtx) {
    exCtx.terminate();
    exCtxs.remove(exCtx);
  }

  /**
   * terminates all open execution context.
   */
  public void abort() {
    for (TrainDBExecContext exCtx : exCtxs) {
      exCtx.terminate();
    }
  }

  /**
   * Returns a reliable result set as an answer. Right now, simply returns the first batch of
   * Continuous results.
   *
   * <p>Automatically spawns an independent execution context, then runs a query using it.
   *
   * @param query Either a select query or a create-scramble query
   * @return A single query result is returned. If the query is a create-scramble query,
   *     the number of inserted rows are returned.
   * @throws TrainDBException
   */
  public VerdictSingleResult sql(String query) throws TrainDBException {
    TrainDBExecContext exCtx = createTrainDBExecContext();
    VerdictSingleResult result = exCtx.sql(query, false);
    removeTrainDBExecContext(exCtx);
    return result;
  }

  /**
   * Returns a progressive query result set as an answer.
   *
   * @param query Either a select query or a create-scramble query.
   * @return Reader enables progressive query result consumption.
   *     If this is a create-scramble query, the number of inserted rows are returned.
   * @throws TrainDBException
   */
  public VerdictResultStream streamsql(String query) throws TrainDBException {
    TrainDBExecContext exCtx = createTrainDBExecContext();
    VerdictResultStream stream = exCtx.streamsql(query);
    return stream;
  }
}
