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

package traindb.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.hadoop.service.AbstractService;
import traindb.common.TrainDBLogger;


public final class TaskCoordinator extends AbstractService {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TaskCoordinator.class);
  private static TaskCoordinator singletonInstance;

  // for incremental query
  public List<String> saveQuery;
  public int saveQueryIdx;
  public List<List<Object>> totalRes;
  public List<String> header;
  public List<SqlAggFunction> aggCalls;

  // for parallel incremental query
  private boolean isParallel;
  private List<Future<List<List<Object>>>> futures;

  public void setParallel(boolean p) {
    isParallel = p;
  }

  public boolean isParallel() {
    return isParallel;
  }

  public List<Future<List<List<Object>>>> getFutures() {
    return futures;
  }

  public void setFutures(List<Future<List<List<Object>>>> futures) {
    this.futures = futures;
  }

  private TaskCoordinator() {
    super(TaskCoordinator.class.getName());

    saveQuery = new ArrayList<>();
    totalRes = new ArrayList<>();
    header = new ArrayList<>();
    aggCalls = new ArrayList<>();

    isParallel = false;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("stop service - " + getName());
    singletonInstance = null;
    super.serviceStop();
  }

  public static TaskCoordinator getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new TaskCoordinator();
    }
    return singletonInstance;
  }

}
