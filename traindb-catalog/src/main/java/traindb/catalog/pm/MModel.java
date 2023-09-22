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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Unique;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
public final class MModel {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Unique(name = "NAME_IDX")
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String model_name;

  @Persistent(dependent = "false")
  private MModeltype modeltype;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String schema_name;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String table_name;

  @Persistent
  private long table_rows;

  @Persistent
  private long trained_rows;

  @Persistent
  private List<String> columns;

  @Persistent
  @Column(length = 8)  // 'ENABLED', 'DISABLED'
  private String model_status;

  @Persistent
  private byte[] model_options;

  @Persistent(mappedBy = "model", dependentElement = "true")
  private Collection<MTrainingStatus> training_status;

  @Persistent(dependent = "false")
  private MTable table;

  public MModel(
      MModeltype modeltype, String modelName, String schemaName, String tableName,
      List<String> columns, @Nullable Long baseTableRows, @Nullable Long trainedRows,
      String options, MTable table) {
    this.modeltype = modeltype;
    this.model_name = modelName;
    this.schema_name = schemaName;
    this.table_name = tableName;
    this.columns = columns;
    this.table_rows = (baseTableRows == null) ? 0 : baseTableRows;
    this.trained_rows = (trainedRows == null) ? 0 : trainedRows;
    this.model_status = "ENABLED";  // initial status
    this.model_options = options.getBytes();
    this.table = table;
  }

  public String getModelName() {
    return model_name;
  }

  public MModeltype getModeltype() {
    return modeltype;
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

  public long getTableRows() {
    return table_rows;
  }

  public long getTrainedRows() {
    return trained_rows;
  }

  public String getModelOptions() {
    return new String(model_options);
  }

  public Collection<MTrainingStatus> trainingStatus() {
    return training_status;
  }

  public MTable getTable() {
    return table;
  }

  public String getModelStatus() {
    return model_status;
  }

  public void setModelName(String modelName) {
    this.model_name = modelName;
  }

  public void enableModel() {
    this.model_status = "ENABLED";
  }

  public void disableModel() {
    this.model_status = "DISABLED";
  }

  public boolean isEnabled() {
    return model_status.equals("ENABLED");
  }

  public boolean isTrainingFinished() {
    if (training_status.isEmpty() || training_status.size() == 0) {
      return true;
    }
    Comparator<MTrainingStatus> comparator = Comparator.comparing(MTrainingStatus::getStartTime);
    MTrainingStatus latestStatus = training_status.stream().max(comparator).get();
    return latestStatus.getTrainingStatus().equals("FINISHED");
  }
}
