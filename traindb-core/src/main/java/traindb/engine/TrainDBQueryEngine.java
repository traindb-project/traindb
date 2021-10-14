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

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.simple.JSONObject;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.coordinator.VerdictSingleResultFromListData;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.CatalogStore;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModelInstance;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.sql.TrainDBSqlRunner;


public class TrainDBQueryEngine implements TrainDBSqlRunner {

  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());
  private DbmsConnection conn;
  private CatalogContext catalogContext;
  private TrainDBConfiguration conf;

  public TrainDBQueryEngine(DbmsConnection conn, CatalogStore catalogStore,
                            TrainDBConfiguration conf) {
    this.conn = conn;
    this.catalogContext = catalogStore.getCatalogContext();
    this.conf = conf;
  }

  @Override
  public void createModel(String modelName, String modelType, String modelLocation,
                          String modelClassName, String modelUri) throws Exception {
    if (catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' already exists");
    }
    catalogContext.createModel(modelName, modelType, modelLocation, modelClassName, modelUri);
  }

  @Override
  public void dropModel(String modelName) throws Exception {
    if (!catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' does not exist");
    }
    catalogContext.dropModel(modelName);
  }

  private JSONObject getTableMetadata(String schemaName, String tableName,
                                      List<String> columnNames) throws Exception {
    // query to get table metadata
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (String columnName : columnNames) {
      sb.append(columnName);
      sb.append(",");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(" FROM ");
    sb.append(schemaName);
    sb.append(".");
    sb.append(tableName);
    sb.append(" WHERE 1<0");

    String sql = sb.toString();
    DbmsQueryResult res = conn.execute(sql);
    List<String> primaryKeys = conn.getPrimaryKey(schemaName, tableName);

    JSONObject root = new JSONObject();
    JSONObject fields = new JSONObject();
    for (int i = 0; i < res.getColumnCount(); i++) {
      JSONObject typeInfo = new JSONObject();

      /* datatype (type, subtype)
        ('categorical', None): 'object',
        ('boolean', None): 'bool',
        ('numerical', None): 'float',
        ('numerical', 'float'): 'float',
        ('numerical', 'integer'): 'int',
        ('datetime', None): 'datetime64',
        ('id', None): 'int',
        ('id', 'integer'): 'int',
        ('id', 'string'): 'str'
       */
      switch (res.getColumnType(i)) {
        case Types.CHAR:
        case Types.VARCHAR:
          typeInfo.put("type", "categorical");
          break;
        case Types.NUMERIC:
        case Types.DECIMAL:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.TINYINT:
        case Types.SMALLINT:
          typeInfo.put("type", "numerical");
          typeInfo.put("subtype", "integer");
          break;
        case Types.FLOAT:
        case Types.DOUBLE:
          typeInfo.put("type", "numerical");
          typeInfo.put("subtype", "float");
          break;
        case Types.BOOLEAN:
          typeInfo.put("type", "boolean");
          break;
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE:
          typeInfo.put("type", "datetime");
          break;
      }
      for (String pk : primaryKeys) {
        if (res.getColumnName(i).equals(pk)) {
          typeInfo.put("type", "id");
          break;
        }
      }

      fields.put(res.getColumnName(i), typeInfo);
    }
    root.put("fields", fields);
    root.put("schema", schemaName);
    root.put("table", tableName);

    return root;
  }

  // FIXME temporary
  private DbmsQueryResult getTrainingData(String schemaName, String tableName,
                                              List<String> columnNames) throws Exception {
    // query to get table metadata
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (String columnName : columnNames) {
      sb.append(columnName);
      sb.append(",");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(" FROM ");
    sb.append(schemaName);
    sb.append(".");
    sb.append(tableName);

    String sql = sb.toString();
    DbmsQueryResult res = conn.execute(sql);
    return res;
  }

  @Override
  public void trainModelInstance(
      String modelName, String modelInstanceName, String schemaName, String tableName,
      List<String> columnNames) throws Exception {
    if (catalogContext.modelInstanceExists(modelInstanceName)) {
      throw new CatalogException("model instance '" + modelInstanceName + "' already exist");
    }

    JSONObject tableMetadata = getTableMetadata(schemaName, tableName, columnNames);
    MModel mModel = catalogContext.getModel(modelName);
    Path instancePath = catalogContext.getModelInstancePath(modelName, modelInstanceName);
    Files.createDirectories(instancePath);
    String outputPath = instancePath.toString();

    // write metadata for model training scripts in python
    String metadataFilename = outputPath + "/metadata.json";
    FileWriter fileWriter = new FileWriter(metadataFilename);
    fileWriter.write(tableMetadata.toJSONString());
    fileWriter.flush();
    fileWriter.close();

    // FIXME securely pass training data for ML model training
    DbmsQueryResult trainingData = getTrainingData(schemaName, tableName, columnNames);
    String dataFilename = outputPath + "/data.csv";
    FileWriter datafileWriter = new FileWriter(dataFilename);
    datafileWriter.write(new VerdictSingleResultFromDbmsQueryResult(trainingData).toCsv());
    datafileWriter.flush();
    datafileWriter.close();

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python", conf.getModelRunnerPath(), "train",
        mModel.getClassName(), mModel.getUri(), dataFilename, metadataFilename, outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    catalogContext.trainModelInstance(
        modelName, modelInstanceName, schemaName, tableName, columnNames);
  }

  @Override
  public void dropModelInstance(String modelInstanceName) throws Exception {
    if (!catalogContext.modelInstanceExists(modelInstanceName)) {
      throw new CatalogException("model instance '" + modelInstanceName + "' does not exist");
    }
    catalogContext.dropModelInstance(modelInstanceName);
  }

  private void createSynopsisTable(String synopsisName, MModelInstance mModelInstance)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(mModelInstance.getSchemaName());
    sb.append(".");
    sb.append(synopsisName);
    sb.append(" AS SELECT ");
    for (String columnName : mModelInstance.getColumnNames()) {
      sb.append(columnName);
      sb.append(",");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(" FROM ");
    sb.append(mModelInstance.getSchemaName());
    sb.append(".");
    sb.append(mModelInstance.getTableName());
    sb.append(" WHERE 1<0");

    String sql = sb.toString();
    conn.execute(sql);
  }

  private void loadSynopsisIntoTable(DbmsConnection dbmsConn, String synopsisName,
                                     MModelInstance mModelInstance, String synopsisFile)
      throws Exception {
    Connection origConn = null;
    if (dbmsConn instanceof CachedDbmsConnection) {
      loadSynopsisIntoTable(((CachedDbmsConnection) dbmsConn).getOriginalConnection(),
          synopsisName, mModelInstance, synopsisFile);
      return;
    } else if (dbmsConn instanceof JdbcConnection) {
      origConn = ((JdbcConnection) dbmsConn).getConnection();
    } else {
      throw new TrainDBException("cannot load synopsis data into table");
    }

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(mModelInstance.getSchemaName());
    sb.append(".");
    sb.append(synopsisName);
    sb.append(" VALUES (");
    for (String columnName : mModelInstance.getColumnNames()) {
      sb.append("?,");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(")");

    CSVReader csvReader = new CSVReaderBuilder(new FileReader(synopsisFile))
        .withSkipLines(1).build();
    String sql = sb.toString();

    PreparedStatement pstmt = origConn.prepareStatement(sql);
    int collen = mModelInstance.getColumnNames().size();
    String[] row;
    try {
      while ((row = csvReader.readNext()) != null) {
        for (int i = 1; i <= collen; i++) {
          pstmt.setObject(i, row[i - 1]);
        }
        pstmt.addBatch();
      }
      pstmt.executeBatch();
    } catch (Exception e) {
      throw e;
    } finally {
      if (pstmt != null) {
        pstmt.close();
      }
    }
  }

  @Override
  public void createSynopsis(String synopsisName, String modelInstanceName, int limitNumber)
      throws Exception {
    if (!catalogContext.modelInstanceExists(modelInstanceName)) {
      throw new CatalogException("model instance '" + modelInstanceName + "' does not exist");
    }
    MModelInstance mModelInstance = catalogContext.getModelInstance(modelInstanceName);
    MModel mModel = mModelInstance.getModel();
    String instancePath =
        catalogContext.getModelInstancePath(mModel.getName(), mModelInstance.getName()).toString();
    String outputPath = instancePath + '/' + synopsisName + ".csv";

    // generate synopsis from ML model
    ProcessBuilder pb = new ProcessBuilder("python", conf.getModelRunnerPath(), "synopsis",
        mModel.getClassName(), mModel.getUri(), instancePath, String.valueOf(limitNumber),
        outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    // create synopsis table from generated synopsis file
    if (conn.getTables(mModelInstance.getSchemaName()).contains(synopsisName)) {
      throw new CatalogException("table '" + mModelInstance.getSchemaName() + "." + synopsisName
          + "' already exists in source DBMS");
    }
    createSynopsisTable(synopsisName, mModelInstance);
    loadSynopsisIntoTable(conn, synopsisName, mModelInstance, outputPath);

    catalogContext.createSynopsis(synopsisName, modelInstanceName);
  }

  @Override
  public VerdictSingleResult showModels() throws Exception {
    List<String> header = Arrays.asList("model", "type", "location", "class", "uri");
    List<List<Object>> modelInfo = new ArrayList<>();

    for (MModel mModel : catalogContext.getModels()) {
      modelInfo.add(Arrays.asList(mModel.getName(), mModel.getType(), mModel.getLocation(),
          mModel.getClassName(), mModel.getUri()));
    }

    VerdictSingleResult result = new VerdictSingleResultFromListData(header, modelInfo);
    return result;
  }

  @Override
  public VerdictSingleResult showModelInstances(String modelName) throws Exception {
    List<String> header = Arrays.asList("model", "model_instance", "schema", "table", "columns");
    List<List<Object>> modelInstanceInfo = new ArrayList<>();

    for (MModelInstance mModelInstance : catalogContext.getModelInstances(modelName)) {
      modelInstanceInfo.add(Arrays.asList(modelName, mModelInstance.getName(),
          mModelInstance.getSchemaName(), mModelInstance.getTableName(),
          mModelInstance.getColumnNames().toString()));
    }

    VerdictSingleResult result = new VerdictSingleResultFromListData(header, modelInstanceInfo);
    return result;
  }
}
