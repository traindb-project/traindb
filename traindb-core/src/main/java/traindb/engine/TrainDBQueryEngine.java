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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.codec.binary.Hex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import traindb.adapter.TrainDBSqlDialect;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.pm.MColumn;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MQueryLog;
import traindb.catalog.pm.MSynopsis;
import traindb.catalog.pm.MTask;
import traindb.catalog.pm.MTrainingStatus;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.engine.nio.ByteArray;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.SchemaManager;
import traindb.schema.TrainDBTable;
import traindb.sql.TrainDBSqlRunner;
import traindb.task.TaskTracer;
import traindb.util.ZipUtils;


public class TrainDBQueryEngine implements TrainDBSqlRunner {

  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());
  private TrainDBConnectionImpl conn;
  private CatalogContext catalogContext;
  private SchemaManager schemaManager;
  public TaskTracer T_tracer;

  public TrainDBQueryEngine(TrainDBConnectionImpl conn) {
    this.conn = conn;
    this.catalogContext = conn.getCatalogContext();
    this.schemaManager = conn.getSchemaManager();
    this.T_tracer = new TaskTracer();
  }

  @Override
  public void createModeltype(String name, String category, String location,
                              String className, String uri) throws Exception {
    T_tracer.startTaskTracer("create modeltype " + name);

    T_tracer.openTaskTime("find : modeltype");
    if (catalogContext.modeltypeExists(name)) {
      String msg = "modeltype '" + name + "' already exists";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("create modeltype");
    AbstractTrainDBModelRunner runner = createModelRunner(name, "", location);
    String hyperparamsInfo = runner.listHyperparameters(className, uri);
    catalogContext.createModeltype(name, category, location, className, uri, hyperparamsInfo);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void dropModeltype(String name) throws Exception {
    T_tracer.startTaskTracer("drop modeltype " + name);

    T_tracer.openTaskTime("find : modeltype");
    if (!catalogContext.modeltypeExists(name)) {
      String msg = "modeltype '" + name + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("drop modeltype");
    catalogContext.dropModeltype(name);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void trainModel(
      String modeltypeName, String modelName, String schemaName, String tableName,
      List<String> columnNames, Map<String, Object> trainOptions) throws Exception {
    T_tracer.startTaskTracer("train model " + modelName);

    T_tracer.openTaskTime("find : modeltype");
    if (!catalogContext.modeltypeExists(modeltypeName)) {
      String msg = "modeltype '" + modeltypeName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("find : model");
    if (catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' already exists";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    if (schemaName == null) {
      schemaName = conn.getSchema();
    }

    TrainDBTable table = schemaManager.getTable(schemaName, tableName);
    if (table == null) {
      throw new TrainDBException("cannot find the table '" + schemaName + "'.'" + tableName + "'");
    }
    Long baseTableRows = getTableRowCount(schemaName, tableName);
    Long trainedRows = baseTableRows; // TODO

    T_tracer.openTaskTime("train model");
    AbstractTrainDBModelRunner runner = createModelRunner(
        modeltypeName, modelName, catalogContext.getModeltype(modeltypeName).getLocation());
    runner.trainModel(table, columnNames, trainOptions, conn.getTypeFactory());
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("insert model info");
    JSONObject options = new JSONObject();
    options.putAll(trainOptions);
    catalogContext.trainModel(modeltypeName, modelName, schemaName, tableName, columnNames,
        table.getRowType(conn.getTypeFactory()), baseTableRows, trainedRows, options.toString());
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void dropModel(String modelName) throws Exception {
    T_tracer.startTaskTracer("drop model " + modelName);

    T_tracer.openTaskTime("find : model");
    if (!catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("drop model");
    catalogContext.dropModel(modelName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  private void createSynopsisTable(String synopsisName, MModel mModel)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(mModel.getSchemaName());
    sb.append(".");
    sb.append(synopsisName);

    SqlDialect dialect = schemaManager.getDialect();
    if (dialect instanceof TrainDBSqlDialect
        && !((TrainDBSqlDialect) dialect).supportCreateTableAsSelect()) {
      sb.append("(");
      for (String columnName : mModel.getColumnNames()) {
        MColumn mColumn = mModel.getTable().getColumn(columnName);
        if (mColumn == null) {
          throw new TrainDBException("cannot find base column information");
        }
        sb.append(columnName).append(" ");
        SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(mColumn.getColumnType());
        switch (sqlTypeName) {
          case CHAR:
          case VARCHAR:
          case INTEGER:
          case BIGINT:
          case TINYINT:
          case SMALLINT:
          case FLOAT:
          case DOUBLE:
          case DECIMAL:
          case BOOLEAN:
          case DATE:
          case TIME:
          case TIMESTAMP:
            sb.append(sqlTypeName.getName());
            if (sqlTypeName.allowsPrec()) {
              sb.append("(").append(mColumn.getPrecision());
              if (sqlTypeName.allowsScale()) {
                sb.append(",").append(mColumn.getScale());
              }
              sb.append(")");
            }
            break;
          case GEOMETRY: {
            String columnType = "geometry";
            try {
              DatabaseMetaData md = conn.getMetaData();
              ResultSet rs = md.getColumns(
                  "traindb", mModel.getSchemaName(), mModel.getTableName(), columnName);
              while (rs.next()) {
                columnType = rs.getString("TYPE_NAME");
              }
              sb.append(columnType);
            } catch (SQLException e) {
              // ignore
            }
            break;
          }
          default:
            throw new TrainDBException("column type '" + sqlTypeName.getName() + "' not supported");
        }
        if (!mColumn.isNullable()) {
          sb.append(" NOT NULL");
        }
        sb.append(",");
      }
      sb.deleteCharAt(sb.lastIndexOf(","));
      sb.append(")");
    } else {
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
    }

    String sql = sb.toString();
    conn.executeInternal(sql);
    conn.refreshRootSchema();
  }

  private long getTableRowCount(String schemaName, String tableName) throws SQLException {
    String sql = "SELECT count(*) FROM " + schemaName + "." + tableName;
    ResultSet rs = conn.executeQueryInternal(sql);
    long rowCount = 0;
    while (rs.next()) {
      rowCount = rs.getLong(1);
    }
    return rowCount;
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

  private void renameSynopsisTable(String synopsisName, String newSynopsisName)
      throws Exception {
    MSynopsis mSynopsis = catalogContext.getSynopsis(synopsisName);
    String schemaName = mSynopsis.getModel().getSchemaName();

    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ")
        .append(schemaName).append(".").append(synopsisName)
        .append(" RENAME TO ")
        .append(schemaName).append(".").append(newSynopsisName);

    String sql = sb.toString();
    conn.executeInternal(sql);
    conn.refreshRootSchema();
  }

  private AbstractTrainDBModelRunner createModelRunner(String modeltypeName, String modelName,
                                                       String location) {
    return AbstractTrainDBModelRunner.createModelRunner(
        conn, catalogContext, conn.cfg, modeltypeName, modelName, location);
  }

  @Override
  public void createSynopsis(String synopsisName, String modelName,
                             int limitRows, float limitPercent)
      throws Exception {

    T_tracer.startTaskTracer("create synopsis " + synopsisName);

    T_tracer.openTaskTime("find : synopsis");
    if (catalogContext.synopsisExists(synopsisName)) {
      String msg = "synopsis '" + synopsisName + "' already exists";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("find : model(" + modelName + ")");
    if (!catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("generate synopsis");
    MModel mModel = catalogContext.getModel(modelName);
    MModeltype mModeltype = mModel.getModeltype();

    if (!mModel.isEnabled()) {
      throw new TrainDBException("model '" + modelName + "' is disabled");
    }

    AbstractTrainDBModelRunner runner = createModelRunner(
        mModeltype.getModeltypeName(), modelName, mModeltype.getLocation());

    if (!mModel.isTrainingFinished()) {  // remote model
      if (!runner.checkAvailable(modelName)) {
        throw new TrainDBException(
            "model '" + modelName + "' is not available (training is not finished)");
      }
      catalogContext.updateTrainingStatus(modelName, "FINISHED");
    }
    String outputPath = runner.getModelPath().toString() + '/' + synopsisName + ".csv";
    if (limitRows == 0) {
      limitRows =  (int) Math.floor((double) mModel.getTableRows() * limitPercent * 0.01);
    }
    runner.generateSynopsis(outputPath, limitRows);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("create synopsis");
    double ratio = (double) limitRows / (double) mModel.getTableRows();
    catalogContext.createSynopsis(synopsisName, modelName, limitRows, ratio);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("create synopsis table");
    try {
      createSynopsisTable(synopsisName, mModel);
      T_tracer.closeTaskTime("SUCCESS");

      T_tracer.openTaskTime("load synopsis into table");
      loadSynopsisIntoTable(synopsisName, mModel, outputPath);
      T_tracer.closeTaskTime("SUCCESS");
    } catch (Exception e) {
      dropSynopsisTable(synopsisName);

      String msg = "failed to create synopsis " + synopsisName;
      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new TrainDBException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();
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
    T_tracer.startTaskTracer("drop synopsis " + synopsisName);
    T_tracer.openTaskTime("find : synopsis");

    if (!catalogContext.synopsisExists(synopsisName)) {
      String msg = "synopsis '" + synopsisName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("drop table");
    dropSynopsisTable(synopsisName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("delete catalog");
    catalogContext.dropSynopsis(synopsisName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public TrainDBListResultSet showModeltypes(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("modeltype_name", "category", "location", "class_name",
        "uri");
    checkShowWhereColumns(filterPatterns, header);

    T_tracer.startTaskTracer("show modeltypes");
    T_tracer.openTaskTime("scan : modeltype");

    List<List<Object>> modeltypeInfo = new ArrayList<>();
    for (MModeltype mModeltype : catalogContext.getModeltypes(filterPatterns)) {
      modeltypeInfo.add(Arrays.asList(mModeltype.getModeltypeName(), mModeltype.getCategory(),
          mModeltype.getLocation(), mModeltype.getClassName(), mModeltype.getUri()));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, modeltypeInfo);
  }

  @Override
  public TrainDBListResultSet showModels(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("model_name", "modeltype_name", "schema_name", "table_name",
        "columns", "table_rows", "trained_rows", "model_status", "model_options");
    checkShowWhereColumns(filterPatterns, header);
    addPrefixToPatternFilter(filterPatterns, ImmutableList.of("modeltype_name"), "modeltype");

    T_tracer.startTaskTracer("show models");
    T_tracer.openTaskTime("scan : model");

    List<List<Object>> modelInfo = new ArrayList<>();
    for (MModel mModel : catalogContext.getModels(filterPatterns)) {
      modelInfo.add(Arrays.asList(mModel.getModelName(), mModel.getModeltype().getModeltypeName(),
          mModel.getSchemaName(), mModel.getTableName(),
          mModel.getColumnNames().toString(), mModel.getTableRows(), mModel.getTrainedRows(),
          mModel.getModelStatus(), mModel.getModelOptions()));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, modelInfo);
  }

  @Override
  public TrainDBListResultSet showSynopses(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("synopsis_name", "model_name", "schema_name", "table_name",
        "columns", "rows", "ratio");
    checkShowWhereColumns(filterPatterns, header);
    addPrefixToPatternFilter(filterPatterns,
        ImmutableList.of("model_name", "schema_name", "table_name", "columns"), "model");

    T_tracer.startTaskTracer("show synopses");
    T_tracer.openTaskTime("scan : synopsis");

    List<List<Object>> synopsisInfo = new ArrayList<>();
    for (MSynopsis mSynopsis : catalogContext.getAllSynopses(filterPatterns)) {
      MModel mModel = mSynopsis.getModel();
      synopsisInfo.add(Arrays.asList(mSynopsis.getSynopsisName(), mModel.getModelName(),
          mModel.getSchemaName(), mModel.getTableName(), mModel.getColumnNames(),
          mSynopsis.getRows(), String.format("%.8f", mSynopsis.getRatio())));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, synopsisInfo);
  }

  @Override
  public TrainDBListResultSet showSchemas(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("schema");
    List<List<Object>> schemaInfo = new ArrayList<>();

    T_tracer.startTaskTracer("show schemas");
    T_tracer.openTaskTime("scan : schema");

    ResultSet rows = conn.getMetaData().getSchemas(conn.getCatalog(), null);

    while (rows.next()) {
      schemaInfo.add(Arrays.asList(rows.getString(1)));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, schemaInfo);
  }

  @Override
  public TrainDBListResultSet showTables(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("table");
    List<List<Object>> tableInfo = new ArrayList<>();

    T_tracer.startTaskTracer("show tables");
    T_tracer.openTaskTime("scan : table");

    ResultSet rs = conn.getMetaData().getTables(
        conn.getCatalog(), conn.getSchema(), null, null);

    while (rs.next()) {
      tableInfo.add(Arrays.asList(rs.getString(3)));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, tableInfo);
  }

  @Override
  public TrainDBListResultSet showHyperparameters(Map<String, Object> filterPatterns)
      throws Exception {
    List<String> header = Arrays.asList("modeltype_name", "hyperparameter_name", "value_type",
        "default_value", "description");
    checkShowWhereColumns(filterPatterns, Arrays.asList("modeltype_name"));

    T_tracer.startTaskTracer("show hyperparameters");
    T_tracer.openTaskTime("scan : modeltype");

    ObjectMapper objectMapper = new ObjectMapper();
    List<List<Object>> hyperparamInfo = new ArrayList<>();
    for (MModeltype mModeltype : catalogContext.getModeltypes(filterPatterns)) {
      if (mModeltype.getHyperparameters().equals("null")) {
        continue;
      }
      List<Hyperparameter> hyperparameterList = Arrays.asList(
          objectMapper.readValue(mModeltype.getHyperparameters(), Hyperparameter[].class));
      for (Hyperparameter hyperparam : hyperparameterList) {
        hyperparamInfo.add(Arrays.asList(mModeltype.getModeltypeName(), hyperparam.getName(),
            hyperparam.getType(), hyperparam.getDefaultValue(), hyperparam.getDescription()));
      }
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, hyperparamInfo);
  }

  @Override
  public TrainDBListResultSet showTrainings(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("model_name", "model_server", "start_time", "status");
    checkShowWhereColumns(filterPatterns, header);
    replacePatternFilterColumn(filterPatterns, "model_server", "model.modeltype.uri");

    T_tracer.startTaskTracer("show trainings");
    T_tracer.openTaskTime("scan : training status");

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    List<List<Object>> trainingInfo = new ArrayList<>();
    for (MTrainingStatus mTraining : catalogContext.getTrainingStatus(filterPatterns)) {
      if (mTraining.getTrainingStatus().equals("TRAINING")) {
        AbstractTrainDBModelRunner runner =
            AbstractTrainDBModelRunner.createModelRunner(
            conn, catalogContext, conn.cfg, mTraining.getModel().getModeltype().getModeltypeName(),
            mTraining.getModelName(), mTraining.getModel().getModeltype().getLocation());
        try {
          if (runner.checkAvailable(mTraining.getModelName())) {
            catalogContext.updateTrainingStatus(mTraining.getModelName(), "FINISHED");
          }
        } catch (Exception e) {
          // ignore
        }
      }
      trainingInfo.add(Arrays.asList(mTraining.getModelName(),
          mTraining.getModel().getModeltype().getUri(),
          mTraining.getStartTime().toLocalDateTime().format(dtf),
          mTraining.getTrainingStatus()));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, trainingInfo);
  }

  @Override
  public void useSchema(String schemaName) throws Exception {
    T_tracer.startTaskTracer("use " + schemaName);
    T_tracer.openTaskTime("use schema");

    ResultSet rs = conn.getMetaData().getSchemas(conn.getCatalog(), schemaName);
    if (!rs.isBeforeFirst()) {
      String msg = "schema '" + schemaName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();
      insertTask();

      throw new CatalogException(msg);
    }
    conn.setSchema(schemaName);

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();
  }

  @Override
  public TrainDBListResultSet describeTable(String schemaName, String tableName) throws Exception {
    List<String> header = Arrays.asList("column name", "column type");
    List<List<Object>> columnInfo = new ArrayList<>();

    T_tracer.startTaskTracer("desc table " + schemaName + "." + tableName);
    T_tracer.openTaskTime("scan : column");

    if (schemaName == null) {
      schemaName = conn.getSchema();
    }
    ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), schemaName, tableName, null);

    while (rs.next()) {
      columnInfo.add(Arrays.asList(rs.getString(4), rs.getString(6)));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, columnInfo);
  }

  @Override
  public void bypassDdlStmt(String stmt) throws Exception {
    T_tracer.startTaskTracer(stmt);
    T_tracer.openTaskTime("execute ddl");

    conn.executeInternal(stmt);
    conn.refreshRootSchema();

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();
  }

  @Override
  public void insertQueryLogs(String start, String user, String query)
      throws Exception {
    if (!conn.cfg.queryLog()) {
      return;
    }
    catalogContext.insertQueryLog(start, user, query);
  }

  @Override
  public TrainDBListResultSet showQueryLogs(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("start", "user", "query");
    List<List<Object>> queryLogInfo = new ArrayList<>();

    for (MQueryLog mQuerylog : catalogContext.getQueryLogs()) {
      queryLogInfo.add(Arrays.asList(mQuerylog.getStartTime(), mQuerylog.getUser(),
          mQuerylog.getQuery()));
    }

    return new TrainDBListResultSet(header, queryLogInfo);
  }

  @Override
  public void insertTask() throws Exception {
    if (!conn.cfg.taskTrace()) {
      return;
    }
    for (MTask mtask : T_tracer.getTaskLog()) {
      catalogContext.insertTask(mtask.getTime(), mtask.getIdx(), mtask.getTask(),
          mtask.getStatus());
    }
  }

  @Override
  public TrainDBListResultSet showTasks(Map<String, Object> filterPatterns) throws Exception {
    List<String> header = Arrays.asList("time", "idx", "task", "status");
    List<List<Object>> taskInfo = new ArrayList<>();

    for (MTask mTask : catalogContext.getTaskLogs()) {
      taskInfo.add(Arrays.asList(mTask.getTime(), mTask.getIdx(),
          mTask.getTask(), mTask.getStatus()));
    }

    return new TrainDBListResultSet(header, taskInfo);
  }

  @Override
  public void deleteQueryLogs(Integer cnt) throws Exception {
    catalogContext.deleteQueryLogs(cnt);
  }

  @Override
  public void deleteTasks(Integer cnt) throws Exception {
    catalogContext.deleteTasks(cnt);
  }

  @Override
  public TrainDBListResultSet exportModel(String modelName) throws Exception {
    T_tracer.startTaskTracer("export model " + modelName);

    T_tracer.openTaskTime("find : model");
    if (!catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("export model");
    MModel mModel = catalogContext.getModel(modelName);
    MModeltype mModeltype = mModel.getModeltype();

    AbstractTrainDBModelRunner runner = createModelRunner(
        mModeltype.getModeltypeName(), modelName, mModeltype.getLocation());

    if (!mModel.isTrainingFinished()) {  // remote model
      if (!runner.checkAvailable(modelName)) {
        throw new TrainDBException(
            "model '" + modelName + "' is not available (training is not finished)");
      }
      catalogContext.updateTrainingStatus(modelName, "FINISHED");
    }

    Path outputPath = Paths.get(runner.getModelPath().getParent().toString(), modelName + ".zip");
    runner.exportModel(outputPath.toString());

    ObjectMapper mapper = new ObjectMapper();
    ZipUtils.addNewFileFromStringToZip("export_metadata.json",
        mapper.writeValueAsString(mModel), outputPath);

    List<String> header = Arrays.asList("export_model");
    List<List<Object>> exportModelInfo = new ArrayList<>();
    ByteArray byteArray = convertFileToByteArray(new File(outputPath.toString()));
    exportModelInfo.add(Arrays.asList(byteArray));

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, exportModelInfo);
  }

  @Override
  public TrainDBListResultSet importModel(String modelName, String modelBinaryString)
      throws Exception {
    T_tracer.startTaskTracer("import model " + modelName);

    T_tracer.openTaskTime("find : model");
    if (catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' already exists";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("decode model binary string");
    byte[] zipModel = Hex.decodeHex(modelBinaryString);
    byte[] exportMetadata = ZipUtils.extractZipEntry(zipModel, "export_metadata.json");
    if (exportMetadata == null) {
      throw new TrainDBException(
          "exported metadata for the model '" + modelName + "' does not found");
    }
    String metadata = new String(exportMetadata, StandardCharsets.UTF_8);
    JSONParser parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(metadata);
    JSONObject jsonModeltype = (JSONObject) json.get("modeltype");
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("find : modeltype");
    String modeltypeName = (String) ((JSONObject) json.get("modeltype")).get("modeltypeName");
    if (!catalogContext.modeltypeExists(modeltypeName)) {
      String msg = "modeltype '" + modeltypeName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    MModeltype mModeltype = catalogContext.getModeltype(modeltypeName);
    if (!mModeltype.getClassName().equals(jsonModeltype.get("className"))) {
      String msg = "the class name of the modeltype '" + modelName + "' is different";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("import model");
    AbstractTrainDBModelRunner runner = createModelRunner(
        mModeltype.getModeltypeName(), modelName, mModeltype.getLocation());
    runner.importModel(zipModel, mModeltype.getUri());
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("insert model info");
    catalogContext.importModel(modeltypeName, modelName, json);
    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return TrainDBListResultSet.empty();
  }

  @Override
  public void renameModel(String modelName, String newModelName) throws Exception {
    T_tracer.startTaskTracer("rename model " + modelName + " to " + newModelName);

    T_tracer.openTaskTime("find : model");
    if (!catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    if (catalogContext.modelExists(newModelName)) {
      String msg = "model '" + newModelName + "'  already exists";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("rename model");
    MModel mModel = catalogContext.getModel(modelName);
    MModeltype mModeltype = mModel.getModeltype();

    AbstractTrainDBModelRunner runner = createModelRunner(
        mModeltype.getModeltypeName(), modelName, mModeltype.getLocation());
    runner.renameModel(newModelName);
    catalogContext.renameModel(modelName, newModelName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void enableModel(String modelName) throws Exception {
    T_tracer.startTaskTracer("enable model " + modelName);

    T_tracer.openTaskTime("find : model");
    if (!catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    T_tracer.openTaskTime("enable model");
    catalogContext.enableModel(modelName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void disableModel(String modelName) throws Exception {
    T_tracer.startTaskTracer("disable model " + modelName);

    T_tracer.openTaskTime("find : model");
    if (!catalogContext.modelExists(modelName)) {
      String msg = "model '" + modelName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    T_tracer.openTaskTime("disable model");
    catalogContext.disableModel(modelName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void renameSynopsis(String synopsisName, String newSynopsisName) throws Exception {
    T_tracer.startTaskTracer("rename synopsis " + synopsisName + " to " + newSynopsisName);

    T_tracer.openTaskTime("find : synopsis");
    if (!catalogContext.synopsisExists(synopsisName)) {
      String msg = "synopsis '" + synopsisName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    if (catalogContext.synopsisExists(newSynopsisName)) {
      String msg = "synopsis '" + newSynopsisName + "'  already exists";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("rename synopsis");
    renameSynopsisTable(synopsisName, newSynopsisName);
    catalogContext.renameSynopsis(synopsisName, newSynopsisName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void enableSynopsis(String synopsisName) throws Exception {
    T_tracer.startTaskTracer("enable synopsis " + synopsisName);

    T_tracer.openTaskTime("find : synopsis");
    if (!catalogContext.synopsisExists(synopsisName)) {
      String msg = "synopsis '" + synopsisName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    T_tracer.openTaskTime("enable synopsis");
    catalogContext.enableSynopsis(synopsisName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  @Override
  public void disableSynopsis(String synopsisName) throws Exception {
    T_tracer.startTaskTracer("disable synopsis " + synopsisName);

    T_tracer.openTaskTime("find : synopsis");
    if (!catalogContext.synopsisExists(synopsisName)) {
      String msg = "synopsis '" + synopsisName + "' does not exist";

      T_tracer.closeTaskTime(msg);
      T_tracer.endTaskTracer();

      throw new CatalogException(msg);
    }

    T_tracer.openTaskTime("disable synopsis");
    catalogContext.disableSynopsis(synopsisName);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.endTaskTracer();
  }

  private ByteArray convertFileToByteArray(File file) throws Exception {
    byte[] bytes;
    FileInputStream inputStream = null;

    try {
      bytes = new byte[(int) file.length()];
      inputStream = new FileInputStream(file);
      inputStream.read(bytes);
    } catch (Exception e) {
      throw e;
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return new ByteArray(bytes);
  }

  private void checkShowWhereColumns(Map<String, Object> patterns, List<String> columns)
      throws TrainDBException {
    for (String key : patterns.keySet()) {
      if (!columns.contains(key)) {
        throw new TrainDBException("column '" + key + "' does not exist. "
            + "Only " + String.join(",", columns) + " can be specified.");
      }
    }
  }

  private void addPrefixToPatternFilter(Map<String, Object> patterns, List<String> columns,
                                        String prefix) {
    for (String key : patterns.keySet()) {
      if (columns.contains(key)) {
        patterns.put(prefix + "." + key, patterns.remove(key));
      }
    }
  }

  private void replacePatternFilterColumn(Map<String, Object> patterns,
                                          String before, String after) {
    for (String key : patterns.keySet()) {
      if (key.equals(before)) {
        patterns.put(after, patterns.remove(before));
        return;
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Hyperparameter {
    private String name;
    private String type;
    private String defaultValue;
    private String description;

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public String getDescription() {
      return description;
    }
  }
}
