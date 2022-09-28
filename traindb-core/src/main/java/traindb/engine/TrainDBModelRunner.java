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

package traindb.engine;

public interface TrainDBModelRunner {

  void init();

  void connect(String driverClass, String url, String user, String password, String jdbcJarPath);

  String trainModel(String sqlTrainingData, String modeltypeClass, String modelTypePath,
                    String jsonTrainingMetadata, String modelPath);

  void generateSynopsis(String modeltypeClass, String modeltypePath, String modelPath,
                        int rowCount, String outputFile);

}