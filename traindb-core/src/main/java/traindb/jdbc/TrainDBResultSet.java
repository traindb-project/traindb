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

import com.google.common.collect.ImmutableList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.runtime.ArrayEnumeratorCursor;
import org.apache.calcite.runtime.ObjectEnumeratorCursor;

/**
 * Implementation of {@link ResultSet}
 * for the Calcite engine.
 */
public class TrainDBResultSet extends AvaticaResultSet {

  TrainDBResultSet(AvaticaStatement statement,
                   CalcitePrepare.CalciteSignature calciteSignature,
                   ResultSetMetaData resultSetMetaData, TimeZone timeZone,
                   Meta.Frame firstFrame) throws SQLException {
    super(statement, null, calciteSignature, resultSetMetaData, timeZone, firstFrame);
  }

  private static Cursor createCursor(ColumnMetaData.AvaticaType elementType,
                                     Iterable iterable) {
    final Enumerator enumerator = Linq4j.iterableEnumerator(iterable);
    //noinspection unchecked
    return !(elementType instanceof ColumnMetaData.StructType)
        || ((ColumnMetaData.StructType) elementType).columns.size() == 1
        ? new ObjectEnumeratorCursor(enumerator)
        : new ArrayEnumeratorCursor(enumerator);
  }

  @Override
  protected TrainDBResultSet execute() throws SQLException {
    // Call driver's callback. It is permitted to throw a RuntimeException.
    TrainDBConnectionImpl connection = getTrainDBConnection();
    final boolean autoTemp = connection.config().autoTemp();
    Handler.ResultSink resultSink = null;
    if (autoTemp) {
      resultSink = () -> {
      };
    }
    connection.getDriver().handler.onStatementExecute(statement, resultSink);

    super.execute();
    return this;
  }

  @Override
  public ResultSet create(ColumnMetaData.AvaticaType elementType,
                          Iterable<Object> iterable) throws SQLException {
    final List<ColumnMetaData> columnMetaDataList;
    if (elementType instanceof ColumnMetaData.StructType) {
      columnMetaDataList = ((ColumnMetaData.StructType) elementType).columns;
    } else {
      columnMetaDataList =
          ImmutableList.of(ColumnMetaData.dummy(elementType, false));
    }
    final CalcitePrepare.CalciteSignature signature =
        (CalcitePrepare.CalciteSignature) this.signature;
    final CalcitePrepare.CalciteSignature<Object> newSignature =
        new CalcitePrepare.CalciteSignature<>(signature.sql,
            signature.parameters, signature.internalParameters,
            signature.rowType, columnMetaDataList, Meta.CursorFactory.ARRAY,
            signature.rootSchema, ImmutableList.of(), -1, null,
            statement.getStatementType());
    ResultSetMetaData subResultSetMetaData =
        new AvaticaResultSetMetaData(statement, null, newSignature);
    final TrainDBResultSet resultSet =
        new TrainDBResultSet(statement, signature, subResultSetMetaData,
            localCalendar.getTimeZone(), new Meta.Frame(0, true, iterable));
    final Cursor cursor = TrainDBResultSet.createCursor(elementType, iterable);
    return resultSet.execute2(cursor, columnMetaDataList);
  }

  // do not make public
  <T> CalcitePrepare.CalciteSignature<T> getSignature() {
    //noinspection unchecked
    return (CalcitePrepare.CalciteSignature) signature;
  }

  // do not make public
  TrainDBConnectionImpl getTrainDBConnection() throws SQLException {
    return (TrainDBConnectionImpl) statement.getConnection();
  }
}
