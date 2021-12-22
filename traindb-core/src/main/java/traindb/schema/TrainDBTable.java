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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import traindb.common.TrainDBLogger;

public abstract class TrainDBTable extends AbstractQueryableTable {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBTable.class);
  private final String name;
  private final TrainDBSchema schema;
  private Schema.TableType tableType;
  private RelProtoDataType protoRowType;

  public TrainDBTable(String name, TrainDBSchema schema, Schema.TableType tableType,
                      RelDataType protoType) {
    super(Object[].class);
    this.name = name;
    this.tableType = tableType;
    this.schema = schema;
    this.protoRowType = RelDataTypeImpl.proto(protoType);
  }

  @Override
  public final Schema.TableType getJdbcTableType() {
    return tableType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return protoRowType.apply(relDataTypeFactory);
  }

  @Override
  public abstract <T> Queryable<T> asQueryable(
      QueryProvider queryProvider, SchemaPlus schema, String tableName);

  public final String getName() {
    return name;
  }

  public final TrainDBSchema getSchema() {
    return schema;
  }
}
