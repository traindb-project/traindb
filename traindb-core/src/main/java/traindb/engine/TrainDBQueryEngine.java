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

package traindb.engine;

import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogStore;
import traindb.common.TrainDBLogger;
import traindb.sql.TrainDBSqlRunner;


public class TrainDBQueryEngine implements TrainDBSqlRunner {

  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());
  private CatalogContext catalogContext;

  public TrainDBQueryEngine(CatalogStore catalogStore) {
    catalogContext = catalogStore.getCatalogContext();
  }


  @Override
  public void dropModel(String modelName) throws Exception {
    catalogContext.dropModel(modelName);
  }
}
