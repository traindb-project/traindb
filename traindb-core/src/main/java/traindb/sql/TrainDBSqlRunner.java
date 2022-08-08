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
import traindb.engine.TrainDBListResultSet;

public interface TrainDBSqlRunner {

  void createModeltype(String name, String category, String location, String className, String uri)
      throws Exception;

  void dropModeltype(String name) throws Exception;

  void trainModelInstance(String modeltypeName, String modelInstanceName, String schemaName,
                          String tableName, List<String> columnNames) throws Exception;

  void dropModelInstance(String modelInstanceName) throws Exception;

  void createSynopsis(String synopsisName, String modelInstanceName, int limitNumber)
      throws Exception;

  void dropSynopsis(String synopsisName) throws Exception;

  TrainDBListResultSet showModeltypes() throws Exception;

  TrainDBListResultSet showModelInstances() throws Exception;

  TrainDBListResultSet showSynopses() throws Exception;

  TrainDBListResultSet showSchemas() throws Exception;

  TrainDBListResultSet showTables() throws Exception;

  void useSchema(String schemaName) throws Exception;

  TrainDBListResultSet describeTable(String schemaName, String tableName) throws Exception;

  void bypassDdlStmt(String stmt) throws Exception;
}

