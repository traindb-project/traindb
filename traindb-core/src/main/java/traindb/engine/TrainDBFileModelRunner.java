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

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.json.simple.JSONObject;
import traindb.adapter.jdbc.JdbcUtils;
import traindb.catalog.CatalogContext;
import traindb.catalog.pm.MModeltype;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.util.ZipUtils;

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
  public void trainModel(JSONObject tableMetadata, String trainingDataQuery) throws Exception {
    // write metadata for model training scripts in python
    Path modelPath = getModelPath();
    Files.createDirectories(modelPath);
    String outputPath = modelPath.toString();
    String metadataFilename = Paths.get(outputPath, "metadata.json").toString();
    FileWriter fileWriter = new FileWriter(metadataFilename);
    fileWriter.write(tableMetadata.toJSONString());
    fileWriter.flush();
    fileWriter.close();

    Connection extConn = conn.getDataSourceConnection();
    Statement stmt = extConn.createStatement();
    ResultSet trainingData = stmt.executeQuery(trainingDataQuery);
    String dataFilename = Paths.get(outputPath, "data.csv").toString();
    writeResultSetToCsv(trainingData, dataFilename);
    JdbcUtils.close(extConn, stmt, trainingData);

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
  }

  @Override
  public void updateModel(JSONObject tableMetadata, String trainingDataQuery, String exModelName)
      throws Exception {
    // write metadata for model training scripts in python
    Path modelPath = getModelPath();
    Files.createDirectories(modelPath);
    String outputPath = modelPath.toString();
    String metadataFilename = Paths.get(outputPath, "metadata.json").toString();
    FileWriter fileWriter = new FileWriter(metadataFilename);
    fileWriter.write(tableMetadata.toJSONString());
    fileWriter.flush();
    fileWriter.close();

    Connection extConn = conn.getDataSourceConnection();
    Statement stmt = extConn.createStatement();
    ResultSet trainingData = stmt.executeQuery(trainingDataQuery);
    String dataFilename = Paths.get(outputPath, "data.csv").toString();
    writeResultSetToCsv(trainingData, dataFilename);
    JdbcUtils.close(extConn, stmt, trainingData);

    MModeltype mModeltype = catalogContext.getModeltype(modeltypeName);
    String exModelPath = getModelPath(exModelName).toString();

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(), "incremental_learn",
        mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
        dataFilename, metadataFilename, exModelPath, outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to update model" + exModelName + " incrementally");
    }
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

  @Override
  public String listHyperparameters(String className, String uri) throws Exception {

    Path modelPath = getModelPath();
    Files.createDirectories(modelPath);
    String outputPath = modelPath.toString() + "/hyperparams.json";

    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(), "list",
        className, TrainDBConfiguration.absoluteUri(uri), outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to list hyperparameters");
    }

    String hyperparamsInfo =
        new String(Files.readAllBytes(Paths.get(outputPath)), StandardCharsets.UTF_8);
    return hyperparamsInfo;
  }

  @Override
  public void exportModel(String outputPath) throws Exception {
    String modelPath = getModelPath().toString();
    ZipUtils.pack(modelPath, outputPath);
  }

  @Override
  public void importModel(byte[] zipModel, String uri) throws Exception {
    String modelPath = getModelPath().toString();
    ZipUtils.unpack(zipModel, modelPath);
  }

  @Override
  public void renameModel(String newModelName) throws Exception {
    String modelPath = getModelPath().toString();
    File oldDir = new File(modelPath);
    File newDir = new File(oldDir.getParent() + File.separator + newModelName);
    oldDir.renameTo(newDir);
  }

  @Override
  public String analyzeSynopsis(JSONObject tableMetadata, String originalDataQuery,
                                String synopsisDataQuery, String synopsisName) throws Exception {
    // write metadata for model training scripts in python
    Path modelPath = getModelPath();
    Files.createDirectories(modelPath);
    String outputPath = modelPath.toString();
    String metadataFilename = Paths.get(outputPath, "metadata.json").toString();
    FileWriter fileWriter = new FileWriter(metadataFilename);
    fileWriter.write(tableMetadata.toJSONString());
    fileWriter.flush();
    fileWriter.close();

    Connection extConn = conn.getDataSourceConnection();
    Statement stmt = extConn.createStatement();

    ResultSet origData = stmt.executeQuery(originalDataQuery);
    String dataFilename = Paths.get(outputPath, "data.csv").toString();
    writeResultSetToCsv(origData, dataFilename);
    origData.close();

    ResultSet synData = stmt.executeQuery(synopsisDataQuery);
    String synFilename = Paths.get(outputPath, "syn.csv").toString();
    writeResultSetToCsv(synData, synFilename);
    JdbcUtils.close(extConn, stmt, synData);

    String analyzeReportPath = modelPath + "/analyze_" + synopsisName + ".json";

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(), "evaluate",
        dataFilename, synFilename, metadataFilename, analyzeReportPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to analyze synopsis '" + synopsisName + "'");
    }

    String analyzeReport =
        new String(Files.readAllBytes(Paths.get(analyzeReportPath)), StandardCharsets.UTF_8);
    return analyzeReport;
  }
}