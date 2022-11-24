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

import traindb.catalog.CatalogConstants;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.Unique;

@PersistenceCapable
public final class MTask {
  @Persistent
  @Column(length = 32)
  private String time;

  @Persistent
  private Integer idx;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String task;

  @Persistent(dependent = "SUCCESS")
  private String status;

  public MTask(String time, Integer idx, String task, String status) {
    this.time   = time;
    this.idx    = idx;
    this.task   = task;
    this.status = status;
  }

  public String getTime() { return time;  }

  public Integer getIdx() { return idx;  }

  public String getTask() {
    return task;
  }

  public String getStatus()   { return status;  }
}
