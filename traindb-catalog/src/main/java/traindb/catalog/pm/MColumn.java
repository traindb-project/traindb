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
import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
@JsonIgnoreProperties({ "table" })
public final class MColumn {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String column_name;

  private int column_type;

  private int precision;

  private int scale;

  private boolean nullable;

  @Persistent(dependent = "false")
  private MTable table;

  public MColumn(String name, int type, int precision, int scale, boolean nullable, MTable table) {
    this.column_name = name;
    this.column_type = type;
    this.precision = precision;
    this.scale = scale;
    this.nullable = nullable;
    this.table = table;
  }

  public long getId() {
    return id;
  }

  public String getColumnName() {
    return column_name;
  }

  public int getColumnType() {
    return column_type;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  public boolean isNullable() {
    return nullable;
  }

  public MTable getTable() {
    return table;
  }

}
