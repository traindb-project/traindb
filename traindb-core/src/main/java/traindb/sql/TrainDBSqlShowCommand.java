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

abstract class TrainDBSqlShowCommand extends TrainDBSqlCommand {
  private String modelName;

  protected TrainDBSqlShowCommand(String modelName) {
    this.modelName = modelName;
  }

  String getModelName() {
    return modelName;
  }

  static class Models extends TrainDBSqlShowCommand {
    Models() {
      super(null);
    }

    @Override
    public Type getType() {
      return Type.SHOW_MODELS;
    }
  }

  static class ModelInstances extends TrainDBSqlShowCommand {
    ModelInstances(String modelName) {
      super(modelName);
    }

    @Override
    public Type getType() {
      return Type.SHOW_MODEL_INSTANCES;
    }
  }

}
