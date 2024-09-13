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

package traindb.catalog;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.simple.JSONObject;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MQueryLog;
import traindb.catalog.pm.MSchema;
import traindb.catalog.pm.MSynopsis;
import traindb.catalog.pm.MTableExt;
import traindb.catalog.pm.MTable;
import traindb.catalog.pm.MTask;
import traindb.catalog.pm.MTrainingStatus;

public interface CatalogContext {

  /* Modeltype */
  boolean modeltypeExists(String name);

  MModeltype getModeltype(String name);

  Collection<MModeltype> getModeltypes() throws CatalogException;

  Collection<MModeltype> getModeltypes(Map<String, Object> filterPatterns) throws CatalogException;

  MModeltype createModeltype(String name, String type, String location, String className,
                             String uri, String hyperparameters) throws CatalogException;

  void dropModeltype(String name) throws CatalogException;

  /* Model */
  MModel trainModel(
      String modeltypeName, String modelName, String schemaName, String tableName,
      List<String> columnNames, RelDataType dataType, Long baseTableRows, Long trainedRows,
      String options) throws CatalogException;

  void dropModel(String name) throws CatalogException;

  Collection<MModel> getModels() throws CatalogException;

  Collection<MModel> getModels(Map<String, Object> filterPatterns) throws CatalogException;

  Collection<MModel> getInferenceModels(String baseSchema, String baseTable)
      throws CatalogException;

  boolean modelExists(String name);

  MModel getModel(String name);

  Collection<MTrainingStatus> getTrainingStatus(Map<String, Object> filterPatterns)
      throws CatalogException;

  void updateTrainingStatus(String modelName, String status) throws CatalogException;

  void importModel(String modeltypeName, String modelName, JSONObject exportMetadata)
      throws CatalogException;

  void renameModel(String modelName, String newModelName) throws CatalogException;

  void enableModel(String modelName) throws CatalogException;

  void disableModel(String modelName) throws CatalogException;

  /* Synopsis */
  MSynopsis createSynopsis(String synopsisName, String modelName, Integer rows, Double ratio,
                           boolean isExternal) throws CatalogException;

  Collection<MSynopsis> getAllSynopses() throws CatalogException;

  Collection<MSynopsis> getAllSynopses(String baseSchema, String baseTable) throws CatalogException;

  Collection<MSynopsis> getAllSynopses(Map<String, Object> filterPatterns) throws CatalogException;

  boolean synopsisExists(String name);

  MSynopsis getSynopsis(String name);

  void dropSynopsis(String name) throws CatalogException;

  void importSynopsis(String synopsisName, boolean isExternal, JSONObject exportMetadata)
      throws CatalogException;

  void renameSynopsis(String synopsisName, String newSynopsisName) throws CatalogException;

  void enableSynopsis(String synopsisName) throws CatalogException;

  void disableSynopsis(String synopsisName) throws CatalogException;

  void updateSynopsisStatistics(String synopsisName, String statistics) throws CatalogException;

  /* External Table */
  MTableExt createExternalTable(String name, String format, String uri) throws CatalogException;

  boolean externalTableExists(String name);

  @Nullable MTableExt getExternalTable(String name);

  /* Schema & Table */
  MSchema getSchema(String name);

  Collection<MSchema> getSchemas() throws CatalogException;

  Collection<MSchema> getSchemas(Map<String, Object> filterPatterns) throws CatalogException;

  MTable getTable(String schemaName, String tableName);

  Collection<MTable> getTables(Map<String, Object> filterPatterns) throws CatalogException;

  /* Querylog */
  Collection<MQueryLog> getQueryLogs() throws CatalogException;

  Collection<MQueryLog> getQueryLogs(Map<String, Object> filterPatterns) throws CatalogException;

  MQueryLog insertQueryLog(String start, String user, String query) throws CatalogException;

  void deleteQueryLogs(Integer cnt) throws CatalogException;

  /* Task */
  Collection<MTask> getTaskLogs() throws CatalogException;

  Collection<MTask> getTaskLogs(Map<String, Object> filterPatterns) throws CatalogException;

  MTask insertTask(String time, Integer idx, String task, String status) throws CatalogException;

  void deleteTasks(Integer cnt) throws CatalogException;

  /* Join */
  MTable createJoinTable(List<String> schemaNames, List<String> tableNames,
                         List<List<String>> columnNames, List<RelDataType> dataTypes,
                         String joinCondition) throws CatalogException;

  void dropJoinTable(String schemaName, String joinTableName) throws CatalogException;


  /* Common */
  void close();
}
