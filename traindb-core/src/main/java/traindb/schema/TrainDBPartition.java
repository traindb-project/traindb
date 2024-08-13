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
//import org.apache.calcite.adapter.java.AbstractQueryableTable;
//import org.apache.calcite.linq4j.QueryProvider;
//import org.apache.calcite.linq4j.Queryable;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rel.type.RelDataTypeImpl;
//import org.apache.calcite.rel.type.RelProtoDataType;
//import org.apache.calcite.schema.Schema;
//import org.apache.calcite.schema.SchemaPlus;
import traindb.common.TrainDBLogger;

public class TrainDBPartition {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBPartition.class);
  private final String name;
  private final TrainDBSchema schema;
  private final List<String> partitionNameMap;
  private final String colmun;

  public TrainDBPartition(String name, TrainDBSchema schema, List<String> partitionNameMap) {
    this.name = name;
    this.schema = schema;
    this.partitionNameMap = partitionNameMap;
    this.colmun = null;
  }

  public TrainDBPartition(String name, TrainDBSchema schema, List<String> partitionNameMap, String column) {
    this.name = name;
    this.schema = schema;
    this.partitionNameMap = partitionNameMap;
    this.colmun = column;
  }

  public final String getName() {
    return name;
  }

  public final TrainDBSchema getSchema() {
    return schema;
  }

  public final List<String> getPartitionNameMap() {
    return partitionNameMap;
  }

  public final String getColumn() {
    return colmun;
  }
}
