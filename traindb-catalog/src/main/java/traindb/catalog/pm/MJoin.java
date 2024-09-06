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

import java.util.List;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public final class MJoin {

  @Persistent
  private long join_table_id;

  @Persistent
  private long src_table_id;

  @Persistent
  private List<String> columns;

  public MJoin(long joinTableId, long srcTableId, List<String> columns) {
    this.join_table_id = joinTableId;
    this.src_table_id = srcTableId;
    this.columns = columns;
  }

  public List<String> getColumnNames() {
    return columns;
  }

}
