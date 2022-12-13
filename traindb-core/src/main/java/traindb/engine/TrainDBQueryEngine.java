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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlDialect;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import traindb.adapter.TrainDBSqlDialect;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MQueryLog;
import traindb.catalog.pm.MSynopsis;
import traindb.catalog.pm.MTask;
import traindb.common.TrainDBException;
import traindb.common.TrainDBLogger;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.SchemaManager;
import traindb.sql.TrainDBSqlRunner;
import traindb.task.TaskTracer;


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
    catalogContext.createModeltype(name, category, location, className, uri);
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

    T_tracer.openTaskTime("train model");
    AbstractTrainDBModelRunner runner = createModelRunner(modeltypeName, modelName);
    String trainInfo = runner.trainModel(schemaName, tableName, columnNames, trainOptions);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("insert model info");
    JSONParser jsonParser = new JSONParser();
    JSONObject jsonTrainInfo = (JSONObject) jsonParser.parse(trainInfo);
    Long baseTableRows = (Long) jsonTrainInfo.get("base_table_rows");
    Long trainedRows = (Long) jsonTrainInfo.get("trained_rows");

    JSONObject options = new JSONObject();
    options.putAll(trainOptions);
    catalogContext.trainModel(modeltypeName, modelName, schemaName, tableName, columnNames,
        baseTableRows, trainedRows, options.toString());
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
      // FIXME set column type as integer temporarily
      sb.append("(");
      for (String columnName : mModel.getColumnNames()) {
        sb.append(columnName + " integer,");
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

  private AbstractTrainDBModelRunner createModelRunner(String modeltypeName, String modelName) {
    String modelrunner = conn.cfg.getModelRunner();
    if (modelrunner.equals("py4j")) {
      return new TrainDBPy4JModelRunner(conn, catalogContext, modeltypeName, modelName);
    }

    return new TrainDBFileModelRunner(conn, catalogContext, modeltypeName, modelName);
  }

  @Override
  public void createSynopsis(String synopsisName, String modelName, int limitNumber)
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

    AbstractTrainDBModelRunner runner = createModelRunner(mModeltype.getName(), modelName);
    String outputPath = runner.getModelPath().toString() + '/' + synopsisName + ".csv";
    runner.generateSynopsis(outputPath, limitNumber);
    T_tracer.closeTaskTime("SUCCESS");

    T_tracer.openTaskTime("create synopsis");
    double ratio = (double) limitNumber / (double) mModel.getBaseTableRows();
    catalogContext.createSynopsis(synopsisName, modelName, limitNumber, ratio);
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
  public TrainDBListResultSet showModeltypes() throws Exception {
    List<String> header = Arrays.asList("modeltype", "category", "location", "class", "uri");
    List<List<Object>> modeltypeInfo = new ArrayList<>();

    T_tracer.startTaskTracer("show modelstypes");
    T_tracer.openTaskTime("scan : modeltype");

    for (MModeltype mModeltype : catalogContext.getModeltypes()) {
      modeltypeInfo.add(Arrays.asList(mModeltype.getName(), mModeltype.getType(),
          mModeltype.getLocation(), mModeltype.getClassName(), mModeltype.getUri()));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, modeltypeInfo);
  }

  @Override
  public TrainDBListResultSet showModels() throws Exception {
    List<String> header = Arrays.asList("model", "modeltype", "schema", "table", "columns",
        "base_table_rows", "trained_rows", "options");
    List<List<Object>> modelInfo = new ArrayList<>();

    T_tracer.startTaskTracer("show models");
    T_tracer.openTaskTime("scan : model");

    for (MModel mModel : catalogContext.getModels()) {
      modelInfo.add(Arrays.asList(mModel.getName(), mModel.getModeltype().getName(),
          mModel.getSchemaName(), mModel.getTableName(),
          mModel.getColumnNames().toString(), mModel.getBaseTableRows(),
          mModel.getTrainedRows(), mModel.getOptions()));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, modelInfo);
  }

  @Override
  public TrainDBListResultSet showSynopses() throws Exception {
    List<String> header = Arrays.asList("synopsis", "model", "schema", "table", "columns",
        "rows", "ratio");
    List<List<Object>> synopsisInfo = new ArrayList<>();

    T_tracer.startTaskTracer("show sysnopeses");
    T_tracer.openTaskTime("scan : synopsis");

    for (MSynopsis mSynopsis : catalogContext.getAllSynopses()) {
      MModel mModel = mSynopsis.getModel();
      synopsisInfo.add(Arrays.asList(mSynopsis.getName(), mModel.getName(),
          mModel.getSchemaName(), mModel.getTableName(), mModel.getColumnNames(),
          mSynopsis.getRows(), String.format("%.8f", mSynopsis.getRatio())));
    }

    T_tracer.closeTaskTime("SUCCESS");
    T_tracer.endTaskTracer();

    return new TrainDBListResultSet(header, synopsisInfo);
  }

  @Override
  public TrainDBListResultSet showSchemas() throws Exception {
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
  public TrainDBListResultSet showTables() throws Exception {
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
  public TrainDBListResultSet showQueryLogs() throws Exception {
    List<String> header = Arrays.asList("start", "user", "query");
    List<List<Object>> queryLogInfo = new ArrayList<>();

    for (MQueryLog mQuerylog : catalogContext.getQueryLog()) {
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
  public TrainDBListResultSet showTasks() throws Exception {
    List<String> header = Arrays.asList("time", "idx", "task", "status");
    List<List<Object>> taskInfo = new ArrayList<>();

    for (MTask mTask : catalogContext.getTaskLog()) {
      taskInfo.add(Arrays.asList(mTask.getTime(), mTask.getIdx(),
              mTask.getTask(), mTask.getStatus()));
    }

    return new TrainDBListResultSet(header, taskInfo);
  }

  @Override
  public void deleteQueryLogs(Integer cnt) throws Exception  {
    catalogContext.deleteQueryLogs(cnt);
  }

  @Override
  public void deleteTasks(Integer cnt) throws Exception {
    catalogContext.deleteTasks(cnt);
  }
}
