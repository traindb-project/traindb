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

package traindb.schema;

import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import traindb.adapter.jdbc.TrainDBJdbcDataSource;
import traindb.common.TrainDBLogger;

public final class TrainDBCatalogTable extends TrainDBTable {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBCatalogTable.class);

  public TrainDBCatalogTable(String name, TrainDBCatalogSchema schema, MetaImpl.MetaTable tableDef,
                             RelDataType protoType) {
    super(name, schema, Schema.TableType.valueOf(tableDef.tableType), protoType);
  }

  @Override
  public String toString() {
    return "TrainDBCatalogTable {" + getName() + "}";
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return new CatalogTableQueryable<>(queryProvider, schema, tableName);
  }

  private class CatalogTableQueryable<T> extends AbstractTableQueryable<T> {
    CatalogTableQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
      super(queryProvider, schema, TrainDBCatalogTable.this, tableName);
    }

    @Override
    public String toString() {
      return "CatalogTableQueryable {table: " + tableName + "}";
    }

    @Override
    public Enumerator<T> enumerator() {
      final JavaTypeFactory typeFactory = ((CalciteConnection) queryProvider).getTypeFactory();
      @SuppressWarnings({"rawtypes", "unchecked"}) final Enumerable<T> enumerable =
          (Enumerable) ResultSetEnumerable.of(null, "");
      return enumerable.enumerator();
    }
  }
}
