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
import javax.jdo.annotations.Unique;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
@JsonIgnoreProperties({ "table" })
public final class MTableExt {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Unique(name = "TABLEEXT_NAME_IDX")
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String table_name;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String external_table_format;

  @Persistent
  @Column(length = CatalogConstants.CONNECTION_STRING_MAX_LENGTH)
  private String external_table_uri;

  @Persistent(dependent = "false")
  private MTable table;

  public MTableExt(String name, String format, String uri, MTable table) {
    this.table_name = name;
    this.external_table_format = format;
    this.external_table_uri = uri;
    this.table = table;
  }

  public String getTableName() {
    return table_name;
  }

  public String getExternalTableFormat() {
    return external_table_format;
  }

  public String getExternalTableUri() {
    return external_table_uri;
  }

  public MTable getTable() {
    return table;
  }
}
