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

class TrainDBSqlCreateSynopsis extends TrainDBSqlCommand {
  private final String synopsisName;
  private final String modelName;
  private final int limitNumber;

  TrainDBSqlCreateSynopsis(String synopsisName, String modelName, int limitNumber) {
    this.synopsisName = synopsisName;
    this.modelName = modelName;
    this.limitNumber = limitNumber;
  }

  String getSynopsisName() {
    return synopsisName;
  }

  String getModelName() {
    return modelName;
  }

  int getLimitNumber() {
    return limitNumber;
  }

  @Override
  public Type getType() {
    return Type.CREATE_SYNOPSIS;
  }
}
