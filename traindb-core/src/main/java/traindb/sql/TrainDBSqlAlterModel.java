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

abstract class TrainDBSqlAlterModel extends TrainDBSqlCommand {

  private final String modelName;
  private final String newModelName;

  protected TrainDBSqlAlterModel(String modelName, String newModelName) {
    this.modelName = modelName;
    this.newModelName = newModelName;
  }

  public String getModelName() {
    return modelName;
  }

  public String getNewModelName() {
    return newModelName;
  }

  static class Rename extends TrainDBSqlAlterModel {
    Rename(String modelName, String newModelName) {
      super(modelName, newModelName);
    }

    @Override
    public Type getType() {
      return Type.ALTER_MODEL_RENAME;
    }
  }

  static class Enable extends TrainDBSqlAlterModel {
    Enable(String modelName) {
      super(modelName, null);
    }

    @Override
    public Type getType() {
      return Type.ALTER_MODEL_ENABLE;
    }
  }

  static class Disable extends TrainDBSqlAlterModel {
    Disable(String modelName) {
      super(modelName, null);
    }

    @Override
    public Type getType() {
      return Type.ALTER_MODEL_DISABLE;
    }
  }
}
