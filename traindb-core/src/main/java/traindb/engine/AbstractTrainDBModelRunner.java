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
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.json.simple.JSONObject;
import traindb.catalog.CatalogContext;
import traindb.common.TrainDBConfiguration;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.TrainDBTable;

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

  public abstract void trainModel(
      TrainDBTable table, List<String> columnNames, Map<String, Object> trainOptions,
      JavaTypeFactory typeFactory) throws Exception;

  public abstract void generateSynopsis(String synopsisName, int rows) throws Exception;

  public abstract String infer(String aggregateExpression, String groupByColumn,
                               String whereCondition) throws Exception;

  public abstract String listHyperparameters(String className, String uri) throws Exception;

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
      Map<String, Object> trainOptions, RelDataType relDataType) {
    JSONObject root = new JSONObject();
    JSONObject fields = new JSONObject();
    for (int i = 0; i < columnNames.size(); i++) {
      RelDataTypeField field = relDataType.getField(columnNames.get(i), true, false);
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
      switch (field.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          typeInfo.put("type", "categorical");
          break;
        case DECIMAL:
        case INTEGER:
        case BIGINT:
        case TINYINT:
        case SMALLINT:
          typeInfo.put("type", "numerical");
          typeInfo.put("subtype", "integer");
          break;
        case FLOAT:
        case DOUBLE:
          typeInfo.put("type", "numerical");
          typeInfo.put("subtype", "float");
          break;
        case BOOLEAN:
          typeInfo.put("type", "boolean");
          break;
        case DATE:
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          typeInfo.put("type", "datetime");
          break;
        default:
          typeInfo.put("type", "unknown");
          break;
      }

      fields.put(columnNames.get(i), typeInfo);
    }
    root.put("fields", fields);
    root.put("schema", schemaName);
    root.put("table", tableName);

    JSONObject options = new JSONObject();
    options.putAll(trainOptions);
    root.put("options", options);

    return root;
  }

  public String getModelName() {
    return modelName;
  }

}
