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

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.json.simple.JSONObject;
import py4j.GatewayServer;
import traindb.catalog.CatalogContext;
import traindb.catalog.pm.MModeltype;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.jdbc.TrainDBConnectionImpl;

public class TrainDBPy4JModelRunner extends AbstractTrainDBModelRunner {

  public TrainDBPy4JModelRunner(
      TrainDBConnectionImpl conn, CatalogContext catalogContext, String modeltypeName,
      String modelName) {
    super(conn, catalogContext, modeltypeName, modelName);
  }

  public static String getModelRunnerPath() {
    return TrainDBConfiguration.getTrainDBPrefixPath() + "/models/TrainDBPy4JModelRunner.py";
  }

  @Override
  public String trainModel(String schemaName, String tableName, List<String> columnNames,
                           Map<String, Object> trainOptions) throws Exception {
    JSONObject tableMetadata = buildTableMetadata(schemaName, tableName, columnNames, trainOptions);
    // write metadata for model training scripts in python
    Path modelPath = getModelPath();
    Files.createDirectories(modelPath);

    int javaPort = getAvailablePort();
    int pythonPort = getAvailablePort();

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(),
        String.valueOf(javaPort), String.valueOf(pythonPort));
    pb.inheritIO();
    Process process = pb.start();

    sleep(1000); // FIXME waiting the python process
    GatewayServer server = new GatewayServer(null, javaPort, pythonPort, 0, 0, null);
    server.start();

    TrainDBModelRunner modelRunner = (TrainDBModelRunner) server.getPythonServerEntryPoint(
        new Class[] { TrainDBModelRunner.class });

    BasicDataSource ds = conn.getDataSource();
    Class jdbcClass = Class.forName(ds.getDriverClassName());
    MModeltype mModeltype = catalogContext.getModeltype(modeltypeName);
    String trainInfo;
    try {
      modelRunner.connect(
          ds.getDriverClassName(), ds.getUrl(), ds.getUsername(), ds.getPassword(),
          jdbcClass.getProtectionDomain().getCodeSource().getLocation().getPath());
      trainInfo = modelRunner.trainModel(
          buildSelectTrainingDataQuery(schemaName, tableName, columnNames),
          mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
          tableMetadata.toJSONString(), modelPath.toString());
    } catch (Exception e) {
      server.shutdown();
      process.destroy();
      e.printStackTrace();
      throw new TrainDBException("failed to train model");
    }
    server.shutdown();
    process.destroy();

    return trainInfo;
  }

  public void generateSynopsis(String outputPath, int rows) throws Exception {
    String modelPath = getModelPath().toString();
    MModeltype mModeltype = catalogContext.getModel(modelName).getModeltype();

    // generate synopsis from ML model
    int javaPort = getAvailablePort();
    int pythonPort = getAvailablePort();

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python", getModelRunnerPath(),
        String.valueOf(javaPort), String.valueOf(pythonPort));
    pb.inheritIO();
    Process process = pb.start();

    sleep(1000); // FIXME waiting the python process
    GatewayServer server = new GatewayServer(null, javaPort, pythonPort, 0, 0, null);
    server.start();

    TrainDBModelRunner modelRunner = (TrainDBModelRunner) server.getPythonServerEntryPoint(
        new Class[] { TrainDBModelRunner.class });

    try {
      modelRunner.generateSynopsis(
          mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
          modelPath, rows, outputPath);
    } catch (Exception e) {
      server.shutdown();
      process.destroy();
      e.printStackTrace();
      throw new TrainDBException("failed to create synopsis");
    }
    server.shutdown();
    process.destroy();
  }

  @Override
  public String infer(String aggregateExpression, String groupByColumn, String whereCondition)
      throws Exception {
    throw new TrainDBException("Not supported yet in Py4J model runner");
  }

  private int getAvailablePort() throws Exception {
    ServerSocket s;
    try {
      s = new ServerSocket(0);
    } catch (IOException e) {
      throw new TrainDBException("failed to get an available port");
    }
    return s.getLocalPort();
  }
}