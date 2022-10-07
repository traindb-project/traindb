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

import java.util.Arrays;
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
  private String name;

  @Persistent(dependent = "false")
  private MModeltype modeltype;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String schemaName;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String tableName;

  @Persistent
  private long baseTableRows;

  @Persistent
  private long trainedRows;

  @Persistent
  private List<String> columns;

  @Persistent
  private byte[] options;

  public MModel(
      MModeltype modeltype, String modelName, String schemaName, String tableName,
      List<String> columns, @Nullable Long baseTableRows, @Nullable Long trainedRows,
      String options) {
    this.modeltype = modeltype;
    this.name = modelName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columns = columns;
    this.baseTableRows = (baseTableRows == null) ? 0 : baseTableRows;
    this.trainedRows = (trainedRows == null) ? 0 : trainedRows;
    this.options = options.getBytes();
  }

  public String getName() {
    return name;
  }

  public MModeltype getModeltype() {
    return modeltype;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getColumnNames() {
    return columns;
  }

  public long getBaseTableRows() {
    return baseTableRows;
  }

  public long getTrainedRows() {
    return trainedRows;
  }

  public String getOptions() {
    return new String(options);
  }
}
