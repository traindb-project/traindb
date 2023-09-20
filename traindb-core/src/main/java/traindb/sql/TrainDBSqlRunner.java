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
import traindb.engine.TrainDBListResultSet;

public interface TrainDBSqlRunner {

  void createModeltype(String name, String category, String location, String className, String uri)
      throws Exception;

  void dropModeltype(String name) throws Exception;

  void trainModel(String modeltypeName, String modelName, String schemaName, String tableName,
                  List<String> columnNames, Map<String, Object> trainOptions) throws Exception;

  void dropModel(String modelName) throws Exception;

  void createSynopsis(String synopsisName, String modelName, int limitNumber) throws Exception;

  void dropSynopsis(String synopsisName) throws Exception;

  TrainDBListResultSet showModeltypes(Map<String, Object> filterPatterns) throws Exception;

  TrainDBListResultSet showModels(Map<String, Object> filterPatterns) throws Exception;

  TrainDBListResultSet showSynopses(Map<String, Object> filterPatterns) throws Exception;

  TrainDBListResultSet showSchemas(Map<String, Object> filterPatterns) throws Exception;

  TrainDBListResultSet showTables(Map<String, Object> filterPatterns) throws Exception;

  TrainDBListResultSet showHyperparameters(Map<String, Object> filterPatterns) throws Exception;

  TrainDBListResultSet showTrainings(Map<String, Object> filterPatterns) throws Exception;

  void useSchema(String schemaName) throws Exception;

  TrainDBListResultSet describeTable(String schemaName, String tableName) throws Exception;

  void bypassDdlStmt(String stmt) throws Exception;

  void insertQueryLogs(String start, String user, String query) throws Exception;

  TrainDBListResultSet showQueryLogs(Map<String, Object> filterPatterns) throws Exception;

  void insertTask() throws Exception;

  TrainDBListResultSet showTasks(Map<String, Object> filterPatterns) throws Exception;

  void deleteQueryLogs(Integer cnt) throws Exception;

  void deleteTasks(Integer cnt) throws Exception;

  TrainDBListResultSet exportModel(String modelName) throws Exception;

  TrainDBListResultSet importModel(String modelName, String modelBinaryString) throws Exception;

  void alterModel(String modelName, String newModelName) throws Exception;
}
