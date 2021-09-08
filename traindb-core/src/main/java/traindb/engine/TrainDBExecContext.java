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

package traindb.engine;

import java.util.List;
import org.antlr.v4.runtime.RecognitionException;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.coordinator.ExecutionContext;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.metastore.VerdictMetaStore;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.sql.TrainDBSql;
import traindb.sql.TrainDBSqlCommand;
import traindb.sql.TrainDBSqlRunner;


public class TrainDBExecContext {

  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());

  TrainDBSqlRunner engine;
  ExecutionContext executionContext;

  public TrainDBExecContext(
      DbmsConnection conn,
      VerdictMetaStore metaStore,
      String contextId,
      long serialNumber,
      VerdictOption options) {
    engine = new TrainDBQueryEngine();
    executionContext = new ExecutionContext(conn, metaStore, contextId, serialNumber, options);
  }

  public VerdictSingleResult sql(String query) throws TrainDBException {
    return this.sql(query, true);
  }

  public VerdictSingleResult sql(String query, boolean getResult) throws TrainDBException {
    LOG.debug("query=" + query);

    // Check input query with TrainDB sql grammar
    List<TrainDBSqlCommand> commands = null;
    try {
      commands = TrainDBSql.parse(query);
    } catch (RecognitionException e) {
      LOG.debug("not a TrainDB statement -> bypass");
    }

    if (commands != null && commands.size() > 0) {
      try {
        return TrainDBSql.run(commands.get(0), engine);
      } catch (Exception e) {
        throw new TrainDBException(
            "failed to run statement: " + query + "\nerror msg: " + e.getMessage());
      }
    }

    // Pass input query to VerdictDB
    try {
      return executionContext.sql(query, getResult);
    } catch (VerdictDBException e) {
      throw new TrainDBException(e.getMessage());
    }
  }

  public VerdictResultStream streamsql(String query) throws TrainDBException {
    try {
      return executionContext.streamsql(query);
    } catch (VerdictDBException e) {
      throw new TrainDBException(e.getMessage());
    }
  }

  public void abort() {
    executionContext.abort();
  }

  public void terminate() {
    executionContext.terminate();
  }

}