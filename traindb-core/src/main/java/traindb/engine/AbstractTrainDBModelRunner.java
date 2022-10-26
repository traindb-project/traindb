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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import traindb.catalog.CatalogContext;
import traindb.common.TrainDBConfiguration;
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

  public abstract String trainModel(String schemaName, String tableName, List<String> columnNames,
                                    Map<String, Object> trainOptions) throws Exception;

  public abstract void generateSynopsis(String synopsisName, int rows) throws Exception;

  public abstract String infer(String aggregateExpression, String groupByColumn,
                               String whereCondition) throws Exception;

  public Path getModelPath() {
    return Paths.get(TrainDBConfiguration.getTrainDBPrefixPath(), "models",
        modeltypeName, modelName);
  }

  protected String buildSelectTrainingDataQuery(String schemaName, String tableName,
                                                List<String> columnNames) {
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

    return sb.toString();
  }

  protected JSONObject buildTableMetadata(
      String schemaName, String tableName, List<String> columnNames,
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

    res.close();
    return root;
  }

  public String getModelName() {
    return modelName;
  }

}
