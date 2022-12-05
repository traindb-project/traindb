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
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.Unique;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
public final class MQueryLog {
  @Persistent
  @Column(length = 32)
  private String start;

  @Persistent
  @Unique(name = "USER_IDX")
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String user;

  @Persistent
  private byte[] query;

  public MQueryLog(String start, String user, String query) {
    this.start = start;
    this.user = user;
    this.query = query.getBytes();
  }

  public String getStartTime() {
    return start;
  }

  public String getUser() {
    return user;
  }

  public String getQuery() {
    return new String(query);
  }
}
