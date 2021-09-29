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

package traindb.sql;

import java.util.List;

class TrainDBSqlTrainModelInstance extends TrainDBSqlCommand {
  private final String modelName;
  private final String modelInstanceName;
  private final String schemaName;
  private final String tableName;
  private final List<String> columnNames;

  TrainDBSqlTrainModelInstance(String modelName, String modelInstanceName, String schemaName,
                               String tableName, List<String> columnNames) {
    this.modelName = modelName;
    this.modelInstanceName = modelInstanceName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnNames = columnNames;
  }

  String getModelName() {
    return modelName;
  }

  String getModelInstanceName() {
    return modelInstanceName;
  }

  String getSchemaName() {
    return schemaName;
  }

  String getTableName() {
    return tableName;
  }

  List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public Type getType() {
    return Type.TRAIN_MODEL_INSTANCE;
  }
}
