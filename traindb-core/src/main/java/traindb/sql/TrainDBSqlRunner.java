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
import org.verdictdb.VerdictSingleResult;

public interface TrainDBSqlRunner {

  void createModel(String modelName, String modelType, String modelLocation, String modelClassName,
                   String modelUri) throws Exception;

  void dropModel(String modelName) throws Exception;

  void trainModelInstance(String modelName, String modelInstanceName, String schemaName,
                          String tableName, List<String> columnNames) throws Exception;

  void dropModelInstance(String modelInstanceName) throws Exception;

  void createSynopsis(String synopsisName, String modelInstanceName, int limitNumber)
      throws Exception;

  void dropSynopsis(String synopsisName) throws Exception;

  VerdictSingleResult showModels() throws Exception;

  VerdictSingleResult showModelInstances() throws Exception;

  VerdictSingleResult showSynopses() throws Exception;

  VerdictSingleResult showSchemas() throws Exception;

  VerdictSingleResult showTables() throws Exception;

  void useSchema(String schemaName) throws Exception;

  VerdictSingleResult describeTable(String schemaName, String tableName) throws Exception;

  VerdictSingleResult processQuery(String query) throws Exception;
}

