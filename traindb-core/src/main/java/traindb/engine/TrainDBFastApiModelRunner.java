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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.text.StringEscapeUtils;
import org.json.simple.JSONObject;
import traindb.catalog.CatalogContext;
import traindb.catalog.pm.MModeltype;
import traindb.common.TrainDBException;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.TrainDBTable;

public class TrainDBFastApiModelRunner extends AbstractTrainDBModelRunner {

  private static final String BOUNDARY = "*****";
  private static final String DOUBLE_HYPHEN = "--";
  private static final String CRLF = "\r\n";

  public TrainDBFastApiModelRunner(
      TrainDBConnectionImpl conn, CatalogContext catalogContext, String modeltypeName,
      String modelName) {
    super(conn, catalogContext, modeltypeName, modelName);
  }

  private static String checkTrailingSlash(String uri) {
    return uri.endsWith("/") ? uri : uri + "/";
  }

  private void addString(DataOutputStream request, String key, String value) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(DOUBLE_HYPHEN).append(BOUNDARY).append(CRLF);
    sb.append("Content-Disposition: form-data; name=\""+ key +"\"").append(CRLF);
    sb.append("Content-Type: plain/text").append(CRLF);
    sb.append(CRLF).append(value).append(CRLF);
    request.writeBytes(sb.toString());
  }

  private void addMetadataFile(DataOutputStream request, JSONObject metadata) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(DOUBLE_HYPHEN).append(BOUNDARY).append(CRLF);
    sb.append("Content-Disposition: form-data; ");
    sb.append("name=\"metadata_file\"; filename=\"metadata.json\"").append(CRLF);
    sb.append("Content-Type: application/json").append(CRLF);
    sb.append(CRLF).append(metadata.toJSONString()).append(CRLF);
    request.writeBytes(sb.toString());
  }

  private void finishMultipartRequest(DataOutputStream request) throws Exception {
    request.writeBytes(DOUBLE_HYPHEN + BOUNDARY + DOUBLE_HYPHEN + CRLF);
    request.flush();
    request.close();
  }

  @Override
  public void trainModel(TrainDBTable table, List<String> columnNames,
                         Map<String, Object> trainOptions, JavaTypeFactory typeFactory)
      throws Exception {
    MModeltype mModeltype = catalogContext.getModeltype(modeltypeName);
    URL url = new URL(checkTrailingSlash(mModeltype.getUri())
        + "modeltype/" + mModeltype.getClassName() + "/train");
    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    httpConn.setRequestMethod("POST");
    httpConn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);
    httpConn.setDoOutput(true);

    BasicDataSource ds = conn.getDataSource();
    String schemaName = table.getSchema().getName();
    String tableName = table.getName();
    String sql = buildSelectTrainingDataQuery(schemaName, tableName, columnNames,
        table.getRowType(typeFactory));

    JSONObject tableMetadata = buildTableMetadata(schemaName, tableName, columnNames, trainOptions,
        table.getRowType(typeFactory));

    OutputStream outputStream = httpConn.getOutputStream();
    DataOutputStream request = new DataOutputStream(outputStream);

    addString(request, "modeltype_class", mModeltype.getClassName());
    addString(request, "model_name", modelName);
    addString(request, "jdbc_driver_class", ds.getDriverClassName());
    addString(request, "db_url", ds.getUrl());
    addString(request, "db_user", ds.getUsername());
    addString(request, "db_pwd", ds.getPassword());
    addString(request, "select_training_data_sql", sql);
    addMetadataFile(request, tableMetadata);
    finishMultipartRequest(request);

    if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new TrainDBException("failed to train model");
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
    }
    System.out.println(response);
  }

  @Override
  public void generateSynopsis(String outputPath, int rows) throws Exception {
    MModeltype mModeltype = catalogContext.getModel(modelName).getModeltype();
    URL url = new URL(checkTrailingSlash(mModeltype.getUri()) + "model/" + modelName + "/synopsis");

    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    httpConn.setRequestMethod("POST");
    httpConn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);
    httpConn.setDoOutput(true);

    OutputStream outputStream = httpConn.getOutputStream();
    DataOutputStream request = new DataOutputStream(outputStream);

    addString(request, "model_name", modelName);
    addString(request, "modeltype_class", mModeltype.getClassName());
    addString(request, "rows", String.valueOf(rows));
    finishMultipartRequest(request);

    if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new TrainDBException("failed to create synopsis");
    }

    Files.createDirectories(Paths.get(outputPath).getParent());
    FileOutputStream fos = new FileOutputStream(outputPath);
    InputStream is = httpConn.getInputStream();
    int read;
    byte[] buf = new byte[32768];
    while ((read = is.read(buf)) > 0) {
      fos.write(buf, 0, read);
    }
    fos.close();
    is.close();
  }

  @Override
  public String infer(String aggregateExpression, String groupByColumn, String whereCondition)
      throws Exception {
    MModeltype mModeltype = catalogContext.getModel(modelName).getModeltype();
    URL url = new URL(checkTrailingSlash(mModeltype.getUri()) + "model/" + modelName + "/infer");

    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    httpConn.setRequestMethod("POST");
    httpConn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);
    httpConn.setDoOutput(true);

    OutputStream outputStream = httpConn.getOutputStream();
    DataOutputStream request = new DataOutputStream(outputStream);

    addString(request, "model_name", modelName);
    addString(request, "modeltype_class", mModeltype.getClassName());
    addString(request, "agg_expr", aggregateExpression);
    addString(request, "group_by_column", groupByColumn);
    addString(request, "where_condition", whereCondition);
    finishMultipartRequest(request);

    if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new TrainDBException("failed to infer '" + aggregateExpression + "'");
    }

    String modelPath = getModelPath().toString();
    UUID queryId = UUID.randomUUID();
    String outputPath = modelPath + "/infer" + queryId + ".csv";

    Files.createDirectories(Paths.get(outputPath).getParent());
    FileOutputStream fos = new FileOutputStream(outputPath);
    InputStream is = httpConn.getInputStream();
    int read;
    byte[] buf = new byte[32768];
    while ((read = is.read(buf)) > 0) {
      fos.write(buf, 0, read);
    }
    fos.close();
    is.close();

    return outputPath;
  }

  private String unescapeString(String s) {
    // remove beginning/ending double quotes and unescape
    return StringEscapeUtils.unescapeJava(s.replaceAll("^\"|\"$", ""));
  }

  @Override
  public String listHyperparameters(String className, String uri) throws Exception {
    URL url = new URL(checkTrailingSlash(uri) + "modeltype/" + className + "/hyperparams");
    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    httpConn.setRequestMethod("GET");

    if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new TrainDBException("failed to list hyperparameters");
    }

    StringBuilder response = new StringBuilder();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      response.append(line);
    }

    return unescapeString(response.toString());
  }

  @Override
  public boolean checkAvailable(String modelName) throws Exception {
    MModeltype mModeltype = catalogContext.getModel(modelName).getModeltype();
    URL url = new URL(checkTrailingSlash(mModeltype.getUri()) + "model/" + modelName + "/status");
    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    httpConn.setRequestMethod("GET");

    if (httpConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new TrainDBException("failed to get model status");
    }

    StringBuilder response = new StringBuilder();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(httpConn.getInputStream(), StandardCharsets.UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      response.append(line);
    }
    String res = unescapeString(response.toString());
    if (res.equalsIgnoreCase("FINISHED")) {
      return true;
    }
    return false;
  }

}