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

class TrainDBSqlCreateModeltype extends TrainDBSqlCommand {
  private final String name;
  private final String category;
  private final String location;
  private final String className;
  private final String uri;

  TrainDBSqlCreateModeltype(String modelName, String modelType, String modelLocation,
                            String modelClassName, String modelUri) {
    this.name = modelName;
    this.category = modelType;
    this.location = modelLocation;
    this.className = modelClassName;
    this.uri = modelUri;
  }

  String getName() {
    return name;
  }

  String getCategory() {
    return category;
  }

  String getLocation() {
    return location;
  }

  String getClassName() {
    return className;
  }

  String getUri() {
    return uri;
  }

  @Override
  public Type getType() {
    return Type.CREATE_MODELTYPE;
  }
}
