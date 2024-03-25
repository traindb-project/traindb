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

package traindb.catalog.pm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Unique;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
@JsonIgnoreProperties({ "model" })
public final class MSynopsis {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Unique(name = "SYNOPSIS_NAME_IDX")
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String synopsis_name;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String model_name;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String schema_name;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String table_name;

  @Persistent
  private List<String> columns;

  @Persistent
  private int rows;

  @Persistent
  private double ratio;

  @Persistent
  @Column(length = 8)  // 'ENABLED', 'DISABLED'
  private String synopsis_status;

  @Persistent
  private byte[] synopsis_statistics;

  @Persistent(dependent = "false")
  private MModel model;

  @Persistent(dependent = "false")
  private MTable table;

  public MSynopsis(String name, Integer rows, Double ratio, MModel model, MTable table) {
    this(name, rows, ratio, model.getModelName(), model.getSchemaName(), model.getTableName(),
        model.getColumnNames(), table);
    this.model = model;
  }

  public MSynopsis(String name, Integer rows, Double ratio, String modelName, String schemaName,
                   String tableName, List<String> columns, MTable table) {
    this.synopsis_name = name;
    this.rows = rows;
    this.ratio = (ratio == null) ? 0 : ratio;
    this.synopsis_status = "ENABLED";  // initial status
    this.model_name = modelName;
    this.schema_name = schemaName;
    this.table_name = tableName;
    this.columns = columns;
    this.table = table;

    this.model = null;
    setSynopsisStatistics("");
  }

  public String getSynopsisName() {
    return synopsis_name;
  }

  public int getRows() {
    return rows;
  }

  public double getRatio() {
    return ratio;
  }

  public MModel getModel() {
    return model;
  }

  public MTable getBaseTable() {
    return table;
  }

  public String getModelName() {
    return model_name;
  }

  public String getSchemaName() {
    return schema_name;
  }

  public String getTableName() {
    return table_name;
  }

  public List<String> getColumnNames() {
    return columns;
  }

  public String getSynopsisStatus() {
    return synopsis_status;
  }

  public String getSynopsisStatistics() {
    return new String(synopsis_statistics);
  }

  public void setSynopsisName(String synopsisName) {
    this.synopsis_name = synopsisName;
  }

  public void enableSynopsis() {
    this.synopsis_status = "ENABLED";
  }

  public void disableSynopsis() {
    this.synopsis_status = "DISABLED";
  }

  public boolean isEnabled() {
    return synopsis_status.equals("ENABLED");
  }

  public void setModelName(String modelName) {
    this.model_name = modelName;
  }

  public void setSchemaName(String schemaName) {
    this.schema_name = schemaName;
  }

  public void setTableName(String tableName) {
    this.table_name = tableName;
  }

  public void setColumnNames(List<String> columns) {
    this.columns = columns;
  }

  public void setSynopsisStatistics(String statistics) {
    this.synopsis_statistics = statistics.getBytes();
  }
}
