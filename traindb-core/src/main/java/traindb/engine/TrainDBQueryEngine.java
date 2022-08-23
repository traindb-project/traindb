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
import com.opencsv.CSVWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MSynopsis;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.SchemaManager;
import traindb.sql.TrainDBSqlRunner;


public class TrainDBQueryEngine implements TrainDBSqlRunner {

  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());
  private TrainDBConnectionImpl conn;
  private CatalogContext catalogContext;
  private SchemaManager schemaManager;

  public TrainDBQueryEngine(TrainDBConnectionImpl conn) {
    this.conn = conn;
    this.catalogContext = conn.getCatalogContext();
    this.schemaManager = conn.getSchemaManager();
  }

  @Override
  public void createModeltype(String name, String category, String location,
                              String className, String uri) throws Exception {
    if (catalogContext.modeltypeExists(name)) {
      throw new CatalogException("modeltype '" + name + "' already exists");
    }
    catalogContext.createModeltype(name, category, location, className, uri);
  }

  @Override
  public void dropModeltype(String name) throws Exception {
    if (!catalogContext.modeltypeExists(name)) {
      throw new CatalogException("modeltype '" + name + "' does not exist");
    }
    catalogContext.dropModeltype(name);
  }

  private JSONObject getTableMetadata(String schemaName, String tableName, List<String> columnNames,
                                      Map<String, Object> trainOptions) throws Exception {
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
    ResultSet res = conn.executeQueryInternal(sql);

    JSONObject root = new JSONObject();
    JSONObject fields = new JSONObject();
    for (int i = 1; i <= res.getMetaData().getColumnCount(); i++) {
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
      switch (res.getMetaData().getColumnType(i)) {
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
        default:
          typeInfo.put("type", "unknown");
          break;
      }

      fields.put(res.getMetaData().getColumnName(i), typeInfo);
    }
    root.put("fields", fields);
    root.put("schema", schemaName);
    root.put("table", tableName);

    JSONObject options = new JSONObject();
    options.putAll(trainOptions);
    root.put("options", options);

    return root;
  }

  // FIXME temporary
  private ResultSet getTrainingData(String schemaName, String tableName,
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
    return conn.executeQueryInternal(sql);
  }

  @Override
  public void trainModel(
      String modeltypeName, String modelName, String schemaName, String tableName,
      List<String> columnNames, Map<String, Object> trainOptions) throws Exception {
    if (!catalogContext.modeltypeExists(modeltypeName)) {
      throw new CatalogException("modeltype '" + modeltypeName + "' does not exist");
    }
    if (catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' already exists");
    }
    if (schemaName == null) {
      schemaName = conn.getSchema();
    }

    JSONObject tableMetadata = getTableMetadata(schemaName, tableName, columnNames, trainOptions);
    Path modelPath = catalogContext.getModelPath(modeltypeName, modelName);
    Files.createDirectories(modelPath);
    String outputPath = modelPath.toString();

    // write metadata for model training scripts in python
    String metadataFilename = outputPath + "/metadata.json";
    FileWriter fileWriter = new FileWriter(metadataFilename);
    fileWriter.write(tableMetadata.toJSONString());
    fileWriter.flush();
    fileWriter.close();

    // FIXME securely pass training data for ML model training
    ResultSet trainingData = getTrainingData(schemaName, tableName, columnNames);
    String dataFilename = outputPath + "/data.csv";
    FileWriter datafileWriter = new FileWriter(dataFilename);
    CSVWriter csvWriter = new CSVWriter(datafileWriter, ',');
    csvWriter.writeAll(trainingData, true);
    csvWriter.close();

    MModeltype mModeltype = catalogContext.getModeltype(modeltypeName);

    // train ML model
    ProcessBuilder pb = new ProcessBuilder("python",
        TrainDBConfiguration.getModelRunnerPath(), "train",
        mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
        dataFilename, metadataFilename, outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to train model " + modelName);
    }

    String trainInfoFilename = outputPath + "/train_info.json";
    JSONParser jsonParser = new JSONParser();
    JSONObject jsonTrainInfo = (JSONObject) jsonParser.parse(new FileReader(trainInfoFilename));
    Long baseTableRows = (Long) jsonTrainInfo.get("base_table_rows");
    Long trainedRows = (Long) jsonTrainInfo.get("trained_rows");

    catalogContext.trainModel(
        modeltypeName, modelName, schemaName, tableName, columnNames,
        baseTableRows, trainedRows, tableMetadata.get("options").toString());
  }

  @Override
  public void dropModel(String modelName) throws Exception {
    if (!catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' does not exist");
    }
    catalogContext.dropModel(modelName);
  }

  private void createSynopsisTable(String synopsisName, MModel mModel)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(mModel.getSchemaName());
    sb.append(".");
    sb.append(synopsisName);
    sb.append(" AS SELECT ");
    for (String columnName : mModel.getColumnNames()) {
      sb.append(columnName);
      sb.append(",");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(" FROM ");
    sb.append(mModel.getSchemaName());
    sb.append(".");
    sb.append(mModel.getTableName());
    sb.append(" WHERE 1<0");

    String sql = sb.toString();
    conn.executeInternal(sql);
    conn.refreshRootSchema();
  }

  private void loadSynopsisIntoTable(String synopsisName, MModel mModel,
                                     String synopsisFile) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(mModel.getSchemaName());
    sb.append(".");
    sb.append(synopsisName);
    sb.append(" VALUES (");
    for (String columnName : mModel.getColumnNames()) {
      sb.append("?,");
    }
    sb.deleteCharAt(sb.lastIndexOf(","));
    sb.append(")");

    CSVReader csvReader = new CSVReaderBuilder(new FileReader(synopsisFile))
        .withSkipLines(1).build();
    String sql = sb.toString();

    PreparedStatement pstmt = conn.prepareInternal(sql);
    int collen = mModel.getColumnNames().size();
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
  public void createSynopsis(String synopsisName, String modelName, int limitNumber)
      throws Exception {
    if (catalogContext.synopsisExists(synopsisName)) {
      throw new CatalogException("synopsis '" + synopsisName + "' already exists");
    }
    if (!catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' does not exist");
    }
    MModel mModel = catalogContext.getModel(modelName);
    MModeltype mModeltype = mModel.getModeltype();
    String modelPath =
        catalogContext.getModelPath(mModeltype.getName(), mModel.getName()).toString();
    String outputPath = modelPath + '/' + synopsisName + ".csv";

    // generate synopsis from ML model
    ProcessBuilder pb = new ProcessBuilder("python",
        TrainDBConfiguration.getModelRunnerPath(), "synopsis",
        mModeltype.getClassName(), TrainDBConfiguration.absoluteUri(mModeltype.getUri()),
        modelPath, String.valueOf(limitNumber), outputPath);
    pb.inheritIO();
    Process process = pb.start();
    process.waitFor();

    if (process.exitValue() != 0) {
      throw new TrainDBException("failed to create synopsis " + synopsisName);
    }
    double ratio = (double) limitNumber / (double) mModel.getBaseTableRows();
    catalogContext.createSynopsis(synopsisName, modelName, limitNumber, ratio);
    try {
      createSynopsisTable(synopsisName, mModel);
      loadSynopsisIntoTable(synopsisName, mModel, outputPath);
    } catch (Exception e) {
      dropSynopsisTable(synopsisName);
      throw new TrainDBException("failed to create synopsis " + synopsisName);
    }
  }

  private void dropSynopsisTable(String synopsisName) throws Exception {
    MSynopsis mSynopsis = catalogContext.getSynopsis(synopsisName);
    StringBuilder sb = new StringBuilder();
    sb.append("DROP TABLE ");
    sb.append(mSynopsis.getModel().getSchemaName());
    sb.append(".");
    sb.append(synopsisName);

    String sql = sb.toString();
    conn.executeInternal(sql);
    conn.refreshRootSchema();
  }

  @Override
  public void dropSynopsis(String synopsisName) throws Exception {
    if (!catalogContext.synopsisExists(synopsisName)) {
      throw new CatalogException("synopsis '" + synopsisName + "' does not exist");
    }
    dropSynopsisTable(synopsisName);
    catalogContext.dropSynopsis(synopsisName);
  }

  @Override
  public TrainDBListResultSet showModeltypes() throws Exception {
    List<String> header = Arrays.asList("modeltype", "category", "location", "class", "uri");
    List<List<Object>> modeltypeInfo = new ArrayList<>();

    for (MModeltype mModeltype : catalogContext.getModeltypes()) {
      modeltypeInfo.add(Arrays.asList(mModeltype.getName(), mModeltype.getType(),
          mModeltype.getLocation(), mModeltype.getClassName(), mModeltype.getUri()));
    }

    return new TrainDBListResultSet(header, modeltypeInfo);
  }

  @Override
  public TrainDBListResultSet showModels() throws Exception {
    List<String> header = Arrays.asList("model", "modeltype", "schema", "table", "columns",
        "base_table_rows", "trained_rows", "options");
    List<List<Object>> modelInfo = new ArrayList<>();

    for (MModel mModel : catalogContext.getModels()) {
      modelInfo.add(Arrays.asList(mModel.getName(), mModel.getModeltype().getName(),
          mModel.getSchemaName(), mModel.getTableName(),
          mModel.getColumnNames().toString(), mModel.getBaseTableRows(),
          mModel.getTrainedRows(), mModel.getOptions()));
    }

    return new TrainDBListResultSet(header, modelInfo);
  }

  @Override
  public TrainDBListResultSet showSynopses() throws Exception {
    List<String> header = Arrays.asList("synopsis", "model", "schema", "table", "columns",
        "rows", "ratio");
    List<List<Object>> synopsisInfo = new ArrayList<>();

    for (MSynopsis mSynopsis : catalogContext.getAllSynopses()) {
      MModel mModel = mSynopsis.getModel();
      synopsisInfo.add(Arrays.asList(mSynopsis.getName(), mModel.getName(),
          mModel.getSchemaName(), mModel.getTableName(), mModel.getColumnNames(),
          mSynopsis.getRows(), String.format("%.8f", mSynopsis.getRatio())));
    }

    return new TrainDBListResultSet(header, synopsisInfo);
  }

  @Override
  public TrainDBListResultSet showSchemas() throws Exception {
    List<String> header = Arrays.asList("schema");
    List<List<Object>> schemaInfo = new ArrayList<>();
    ResultSet rows = conn.getMetaData().getSchemas(conn.getCatalog(), null);

    while (rows.next()) {
      schemaInfo.add(Arrays.asList(rows.getString(1)));
    }

    return new TrainDBListResultSet(header, schemaInfo);
  }

  @Override
  public TrainDBListResultSet showTables() throws Exception {
    List<String> header = Arrays.asList("table");
    List<List<Object>> tableInfo = new ArrayList<>();

    ResultSet rs = conn.getMetaData().getTables(
        conn.getCatalog(), conn.getSchema(), null, null);

    while (rs.next()) {
      tableInfo.add(Arrays.asList(rs.getString(3)));
    }

    return new TrainDBListResultSet(header, tableInfo);
  }

  @Override
  public void useSchema(String schemaName) throws Exception {
    ResultSet rs = conn.getMetaData().getSchemas(conn.getCatalog(), schemaName);
    if (!rs.isBeforeFirst()) {
      throw new CatalogException("schema '" + schemaName + "' does not exist");
    }
    conn.setSchema(schemaName);
  }

  @Override
  public TrainDBListResultSet describeTable(String schemaName, String tableName) throws Exception {
    List<String> header = Arrays.asList("column name", "column type");
    List<List<Object>> columnInfo = new ArrayList<>();
    if (schemaName == null) {
      schemaName = conn.getSchema();
    }
    ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), schemaName, tableName, null);

    while (rs.next()) {
      columnInfo.add(Arrays.asList(rs.getString(4), rs.getString(6)));
    }

    return new TrainDBListResultSet(header, columnInfo);
  }

  @Override
  public void bypassDdlStmt(String stmt) throws Exception {
    conn.executeInternal(stmt);
    conn.refreshRootSchema();
  }
}
