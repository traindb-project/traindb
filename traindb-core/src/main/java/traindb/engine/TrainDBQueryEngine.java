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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.coordinator.VerdictSingleResultFromListData;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.CatalogStore;
import traindb.catalog.pm.MModel;
import traindb.common.TrainDBLogger;
import traindb.sql.TrainDBSqlRunner;


public class TrainDBQueryEngine implements TrainDBSqlRunner {

  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());
  private CatalogContext catalogContext;

  public TrainDBQueryEngine(CatalogStore catalogStore) {
    catalogContext = catalogStore.getCatalogContext();
  }

  @Override
  public void createModel(String modelName, String modelType, String modelLocation, String modelUri)
      throws Exception {
    if (catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' already exists");
    }
    catalogContext.createModel(modelName, modelType, modelLocation, modelUri);
  }

  @Override
  public void dropModel(String modelName) throws Exception {
    if (!catalogContext.modelExists(modelName)) {
      throw new CatalogException("model '" + modelName + "' does not exist");
    }
    catalogContext.dropModel(modelName);
  }

  @Override
  public VerdictSingleResult showModels() throws Exception {
    List<String> header = Arrays.asList("model name", "model type", "model location", "model uri");
    List<List<Object>> modelInfo = new ArrayList<>();

    for (MModel mModel : catalogContext.getModels()) {
      modelInfo.add(Arrays.asList(mModel.getName(), mModel.getType(), mModel.getLocation(),
          mModel.getUri()));
    }

    VerdictSingleResult result = new VerdictSingleResultFromListData(header, modelInfo);
    return result;
  }
}
