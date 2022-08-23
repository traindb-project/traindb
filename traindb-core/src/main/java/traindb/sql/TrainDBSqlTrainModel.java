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
import java.util.Map;

class TrainDBSqlTrainModel extends TrainDBSqlCommand {
  private final String modeltypeName;
  private final String modelName;
  private final String schemaName;
  private final String tableName;
  private final List<String> columnNames;
  private final Map<String, Object> trainOptions;

  TrainDBSqlTrainModel(String modeltypeName, String modelName, String schemaName, String tableName,
                       List<String> columnNames, Map<String, Object> trainOptions) {
    this.modeltypeName = modeltypeName;
    this.modelName = modelName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.trainOptions = trainOptions;
  }

  String getModeltypeName() {
    return modeltypeName;
  }

  String getModelName() {
    return modelName;
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

  Map<String, Object> getTrainOptions() {
    return trainOptions;
  }

  @Override
  public Type getType() {
    return Type.TRAIN_MODEL;
  }
}
