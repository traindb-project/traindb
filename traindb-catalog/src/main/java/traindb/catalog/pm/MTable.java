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

import java.util.ArrayList;
import java.util.Collection;
import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
public final class MTable {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String table_name;

  private String table_type;

  @Persistent(dependent = "false")
  private MSchema schema;

  @Persistent(mappedBy = "table", dependentElement = "true")
  private Collection<MColumn> columns;

  public MTable(String name, String type, MSchema schema) {
    this.table_name = name;
    this.table_type = type;
    this.schema = schema;
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
    return new ArrayList<MColumn>(columns);
  }
}
