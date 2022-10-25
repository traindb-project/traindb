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
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.simple.JSONObject;
import traindb.catalog.CatalogContext;
import traindb.catalog.pm.MModeltype;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.jdbc.TrainDBConnectionImpl;

public class TrainDBFileModelRunner extends AbstractTrainDBModelRunner {

  public TrainDBFileModelRunner(
      TrainDBConnectionImpl conn, CatalogContext catalogContext, String modeltypeName,
      String modelName) {
    super(conn, catalogContext, modeltypeName, modelName);
  }

  public static String getModelRunnerPath() {
    return TrainDBConfiguration.getTrainDBPrefixPath() + "/models/TrainDBCliModelRunner.py";
  }

  @Override
  public String trainModel(String schemaName, String tableName, List<String> columnNames,
                           Map<String, Object> trainOptions) throws Exception {
    JSONObject tableMetadata = buildTableMetadata(schemaName, tableName, columnNames, trainOptions);
    // write metadata for model training scripts in python
    Path modelPath = getModelPath();
    Files.createDirectories(modelPath);
    String outputPath = modelPath.toString();
    String metadataFilename = outputPath + "/metadata.json";
    FileWriter fileWriter = new FileWriter(metadataFilename);
    fileWriter.write(tableMetadata.toJSONString());
    fileWriter.flush();
    fileWriter.close();

    String sql = buildSelectTrainingDataQuery(schemaName, tableName, columnNames);
    ResultSet trainingData = conn.executeQueryInternal(sql);
    String dataFilename = outputPath + "/data.csv";
    FileWriter datafileWriter = new FileWriter(dataFilename);
    CSVWriter csvWriter = new CSVWriter(datafileWriter, ',');
    csvWriter.writeAll(trainingData, true);
    csvWriter.close();
    trainingData.close();

    MModeltype mModeltype = catalogContext.getModeltype(modeltypeName);

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(), "train",
        mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
        dataFilename, metadataFilename, outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to train model");
    }

    Path trainInfoFilePath = Paths.get(outputPath, "train_info.json");
    String trainInfo = new String(Files.readAllBytes(trainInfoFilePath), StandardCharsets.UTF_8);
    return trainInfo;
  }

  @Override
  public void generateSynopsis(String outputPath, int rows) throws Exception {
    String modelPath = getModelPath().toString();
    MModeltype mModeltype = catalogContext.getModel(modelName).getModeltype();

    // generate synopsis from ML model
    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(), "synopsis",
        mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
        modelPath, String.valueOf(rows), outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to create synopsis");
    }
  }

  @Override
  public String infer(String aggregateExpression, String groupByColumn, String whereCondition)
      throws Exception {
    String modelPath = getModelPath().toString();
    MModeltype mModeltype = catalogContext.getModel(modelName).getModeltype();

    UUID queryId = UUID.randomUUID();
    String outputPath = modelPath + "/infer" + queryId + ".csv";

    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(), "infer",
        mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
        modelPath, aggregateExpression, groupByColumn, whereCondition, outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to infer '" + aggregateExpression + "'");
    }

    return outputPath;
  }

}