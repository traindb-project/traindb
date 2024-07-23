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

import java.sql.Timestamp;
import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.Index;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import traindb.catalog.CatalogConstants;

@PersistenceCapable
@Index(name = "TRAINING_STATUS_IDX", members = {"model_name", "start_time"})
public final class MTrainingStatus {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.INCREMENT)
  private long id;

  @Persistent
  @Column(length = CatalogConstants.IDENTIFIER_MAX_LENGTH)
  private String model_name;

  @Persistent
  private Timestamp start_time;

  @Persistent
  @Column(length = 9)
  // Status: TRAINING, FINISHED
  private String training_status;

  @Persistent(dependent = "false")
  private MModel model;

  public MTrainingStatus(String modelName, String status, Timestamp startTime, MModel model) {
    this.model_name = modelName;
    this.training_status = status;
    this.start_time = startTime;
    this.model = model;
  }

  public String getModelName() {
    return model_name;
  }

  public Timestamp getStartTime() {
    return start_time;
  }

  public String getTrainingStatus() {
    return training_status;
  }

  public MModel getModel() {
    return model;
  }

  public void setModelName(String modelName) {
    this.model_name = modelName;
  }

  public void setTrainingStatus(String status) {
    this.training_status = status;
  }
}
