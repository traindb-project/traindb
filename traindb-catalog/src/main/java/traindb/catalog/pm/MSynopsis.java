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
public final class MSynopsis {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Unique(name = "NAME_IDX")
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String name;

  @Persistent
  private int rows;

  @Persistent
  private double ratio;

  @Persistent(dependent = "false")
  private MModel model;

  public MSynopsis(String name, Integer rows, Double ratio, MModel model) {
    this.name = name;
    this.rows = rows;
    this.ratio = (ratio == null) ? 0 : ratio;
    this.model = model;
  }

  public String getName() {
    return name;
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
}
