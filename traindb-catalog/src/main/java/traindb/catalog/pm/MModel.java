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

import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Unique;
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

  @Persistent
  @Column(length = 10) // "synopsis" or "inference"
  private String type;

  @Persistent
  @Column(length = 7) // "local" or "remote"
  private String location;

  @Persistent
  @Column(length = CatalogConstants.CONNECTION_STRING_MAX_LENGTH)
  private String uri;

  public MModel(String name, String type, String location, String uri) {
    this.name = name;
    this.type = type;
    this.location = location;
    this.uri = uri;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getLocation() {
    return location;
  }

  public String getUri() {
    return uri;
  }

}
