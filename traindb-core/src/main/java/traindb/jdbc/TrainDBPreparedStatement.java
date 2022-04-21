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
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link java.sql.PreparedStatement}
 * for the Calcite engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using
 * {@link org.apache.calcite.avatica.AvaticaFactory#newPreparedStatement}.
 */
public abstract class TrainDBPreparedStatement extends AvaticaPreparedStatement {
  /**
   * Creates a CalcitePreparedStatement.
   *
   * @param connection           Connection
   * @param h                    Statement handle
   * @param signature            Result of preparing statement
   * @param resultSetType        Result set type
   * @param resultSetConcurrency Result set concurrency
   * @param resultSetHoldability Result set holdability
   * @throws SQLException if database error occurs
   */
  protected TrainDBPreparedStatement(TrainDBConnectionImpl connection,
                                     Meta.@Nullable StatementHandle h, Meta.Signature signature,
                                     int resultSetType,
                                     int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    super(connection, h, signature, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public TrainDBConnectionImpl getConnection() throws SQLException {
    return (TrainDBConnectionImpl) super.getConnection();
  }
}
