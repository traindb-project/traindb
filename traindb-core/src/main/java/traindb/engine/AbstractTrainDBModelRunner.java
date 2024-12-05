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

import com.opencsv.CSVWriter;
import com.opencsv.ResultSetHelperService;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import org.json.simple.JSONObject;
import traindb.catalog.CatalogContext;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.jdbc.TrainDBConnectionImpl;

public abstract class AbstractTrainDBModelRunner {

  protected TrainDBConnectionImpl conn;
  protected CatalogContext catalogContext;
  protected String modeltypeName;
  protected String modelName;

  public AbstractTrainDBModelRunner(TrainDBConnectionImpl conn, CatalogContext catalogContext,
                                    String modeltypeName, String modelName) {
    this.conn = conn;
    this.catalogContext = catalogContext;
    this.modeltypeName = modeltypeName;
    this.modelName = modelName;
  }

  public abstract void trainModel(JSONObject tableMetadata, String trainingDataQuery)
      throws Exception;

  public abstract void generateSynopsis(String synopsisName, int rows) throws Exception;

  public abstract String infer(String aggregateExpression, String groupByColumn,
                               String whereCondition) throws Exception;

  public abstract String listHyperparameters(String className, String uri) throws Exception;

  public abstract void exportModel(String outputPath) throws Exception;

  public abstract void importModel(byte[] zipModel, String uri) throws Exception;

  public abstract void renameModel(String newModelName) throws Exception;

  public abstract void updateModel(JSONObject tableMetadata, String trainingDataQuery,
                                   String exModelName) throws Exception;

  public String analyzeSynopsis(JSONObject tableMetadata, String originalDataQuery,
      String synopsisDataQuery, String synopsisName) throws Exception {
    throw new TrainDBException("analyzeSynopsis does not supported yet in this model runner");
  }

  public boolean checkAvailable(String modelName) throws Exception {
    return true;
  }

  public String getTrainingStatus(String modelName) throws Exception {
    return "FINISHED";
  }

  public Path getModelPath() {
    return getModelPath(modelName);
  }

  public Path getModelPath(String name) {
    return Paths.get(TrainDBConfiguration.getTrainDBPrefixPath(), "models",
        modeltypeName, name);
  }

  public static AbstractTrainDBModelRunner createModelRunner(
      TrainDBConnectionImpl conn, CatalogContext catalogContext, TrainDBConfiguration config,
      String modeltypeName, String modelName, String location) {
    if (location.equals("REMOTE")) {
      return new TrainDBFastApiModelRunner(conn, catalogContext, modeltypeName, modelName);
    }
    // location.equals("LOCAL")
    if (config.getModelRunner().equals("py4j")) {
      return new TrainDBPy4JModelRunner(conn, catalogContext, modeltypeName, modelName);
    }

    return new TrainDBFileModelRunner(conn, catalogContext, modeltypeName, modelName);
  }

  public static void writeResultSetToCsv(ResultSet rs, String filePath) throws Exception {
    FileWriter datafileWriter = new FileWriter(filePath);
    CSVWriter csvWriter = new CSVWriter(datafileWriter, ',');
    ResultSetHelperService resultSetHelperService = new ResultSetHelperService();
    resultSetHelperService.setDateFormat("yyyy-MM-dd");
    resultSetHelperService.setDateTimeFormat("yyyy-MM-dd HH:MI:SS");
    csvWriter.setResultService(resultSetHelperService);
    csvWriter.writeAll(rs, true);
    csvWriter.close();
  }

  public String getModelName() {
    return modelName;
  }

}
