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
public final class MSchema {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String schema_name;

  @Persistent(mappedBy = "schema", dependentElement = "true", cacheable = "false")
  private Collection<MTable> tables;

  public MSchema(String name) {
    this.schema_name = name;
  }

  public String getSchemaName() {
    return schema_name;
  }

  public Collection<MTable> getTables() {
    return new ArrayList<MTable>(tables);
  }
}
