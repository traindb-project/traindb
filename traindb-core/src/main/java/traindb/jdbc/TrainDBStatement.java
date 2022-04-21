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

package traindb.jdbc;

import java.sql.SQLException;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.server.CalciteServerStatement;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link java.sql.Statement}
 * for the Calcite engine.
 */
public abstract class TrainDBStatement extends AvaticaStatement {
  /**
   * Creates a TrainDBStatement.
   *
   * @param connection           Connection
   * @param h                    Statement handle
   * @param resultSetType        Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   */
  TrainDBStatement(TrainDBConnectionImpl connection, Meta.@Nullable StatementHandle h,
                   int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    super(connection, h, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  // implement Statement

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == CalciteServerStatement.class) {
      final CalciteServerStatement statement;
      try {
        statement = getConnection().server.getStatement(handle);
      } catch (NoSuchStatementException e) {
        throw new AssertionError("invalid statement", e);
      }
      return iface.cast(statement);
    }
    return super.unwrap(iface);
  }

  @Override
  public TrainDBConnectionImpl getConnection() {
    return (TrainDBConnectionImpl) connection;
  }

  public <T> CalcitePrepare.CalciteSignature<T> prepare(
      Queryable<T> queryable) {
    final TrainDBConnectionImpl calciteConnection = getConnection();
    final CalcitePrepare prepare = calciteConnection.prepareFactory.apply();
    final CalciteServerStatement serverStatement;
    try {
      serverStatement = calciteConnection.server.getStatement(handle);
    } catch (NoSuchStatementException e) {
      throw new AssertionError("invalid statement", e);
    }
    final CalcitePrepare.Context prepareContext =
        serverStatement.createPrepareContext();
    return prepare.prepareQueryable(prepareContext, queryable);
  }

  @Override
  protected void close_() {
    if (!closed) {
      ((TrainDBConnectionImpl) connection).server.removeStatement(handle);
      super.close_();
    }
  }
}
