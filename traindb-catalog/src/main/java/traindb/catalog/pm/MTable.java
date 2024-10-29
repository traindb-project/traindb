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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
@JsonIgnoreProperties({ "schema" })
public final class MTable {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String table_name;

  @Persistent
  @Column(length = 18)
  private String table_type;

  @Persistent(dependent = "false")
  private MSchema schema;

  @Persistent(mappedBy = "table", dependentElement = "true")
  private Collection<MColumn> columns;

  @Persistent(mappedBy = "table", dependentElement = "true")
  private Collection<MTableExt> tableExts;

  public MTable(String name, String type, MSchema schema) {
    this.table_name = name;
    this.table_type = type;
    this.schema = schema;
  }

  public long getId() {
    return id;
  }

  public String getTableName() {
    return table_name;
  }

  public String getTableType() {
    return table_type;
  }

  public MSchema getSchema() {
    return schema;
  }

  public Collection<MColumn> getColumns() {
    ArrayList<MColumn> columnList = new ArrayList<>(columns);
    Collections.sort(columnList, new Comparator<MColumn>() {
      public int compare(MColumn c1, MColumn c2) {
        return c1.getId() < c2.getId() ? -1 : 1;
      }
    });
    return columnList;
  }

  public MColumn getColumn(String name) {
    for (MColumn col : columns) {
      if (col.getColumnName().equals(name)) {
        return col;
      }
    }
    return null;
  }

  public Collection<MTableExt> getTableExts() {
    return tableExts;
  }
}
