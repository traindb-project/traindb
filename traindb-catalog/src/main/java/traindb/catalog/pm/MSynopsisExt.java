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
public final class MSynopsisExt {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Unique(name = "SYNOPSIS_NAME_IDX")
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String synopsis_name;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String synopsis_format;

  @Persistent
  @Column(length = CatalogConstants.CONNECTION_STRING_MAX_LENGTH)
  private String synopsis_uri;

  @Persistent(dependent = "false")
  private MSynopsis synopsis;

  public MSynopsisExt(String name, String format, String uri, MSynopsis synopsis) {
    this.synopsis_name = name;
    this.synopsis_format = format;
    this.synopsis_uri = uri;
    this.synopsis = synopsis;
  }

  public String getSynopsisName() {
    return synopsis_name;
  }

  public String getSynopsisFormat() {
    return synopsis_format;
  }

  public String getSynopsisUri() {
    return synopsis_uri;
  }

  public MSynopsis getSynopsis() {
    return synopsis;
  }
}
