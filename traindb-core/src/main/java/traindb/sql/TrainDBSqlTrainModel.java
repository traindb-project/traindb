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
  private final List<String> schemaNames;
  private final List<String> tableNames;
  private final List<List<String>> columnNames;
  private final String tableCondition;
  private final float samplePercent;
  private final Map<String, Object> trainOptions;
  private final String exModelName;
  private final boolean incremental;

  TrainDBSqlTrainModel(String modeltypeName, String modelName, List<String> schemaNames,
                       List<String> tableNames, List<List<String>> columnNames,
                       String tableCondition, float samplePercent,
                       Map<String, Object> trainOptions, String exModelName,
                       boolean incremental) {
    this.modeltypeName = modeltypeName;
    this.modelName = modelName;
    this.schemaNames = schemaNames;
    this.tableNames = tableNames;
    this.columnNames = columnNames;
    this.tableCondition = tableCondition;
    this.samplePercent = samplePercent;
    this.trainOptions = trainOptions;
    this.exModelName = exModelName;
    this.incremental = incremental;
  }

  String getModeltypeName() {
    return modeltypeName;
  }

  String getModelName() {
    return modelName;
  }

  String getExModelName() {
    return exModelName;
  }

  List<String> getSchemaNames() {
    return schemaNames;
  }

  List<String> getTableNames() {
    return tableNames;
  }

  List<List<String>> getColumnNames() {
    return columnNames;
  }

  String getTableCondition() {
    return tableCondition;
  }

  float getSamplePercent() {
    return samplePercent;
  }

  Map<String, Object> getTrainOptions() {
    return trainOptions;
  }

  boolean isIncremental() {
    return incremental;
  }

  @Override
  public Type getType() {
    return Type.TRAIN_MODEL;
  }
}
