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

package traindb.catalog;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import traindb.catalog.pm.MModel;
import traindb.common.TrainDBLogger;

public final class JDOCatalogContext implements CatalogContext {

  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(JDOCatalogContext.class);
  private final PersistenceManager pm;

  public JDOCatalogContext(PersistenceManager persistenceManager) {
    pm = persistenceManager;
  }

  private MModel getMModel(String name) throws CatalogException {
    try {
      Query query = pm.newQuery(MModel.class);
      query.setFilter("name == modelName");
      query.declareParameters("String modelName");
      query.setUnique(true);

      MModel cModel = (MModel) query.execute(name);
      if (cModel == null) {
        throw new CatalogException("model '" + name + "' does not exist");
      }
      return cModel;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get model '" + name + "'", e);
    }
  }

  @Override
  public boolean modelExists(String name) throws CatalogException {
    return getMModel(name) != null;
  }

  @Override
  public MModel getModel(String name) throws CatalogException {
    return getMModel(name);
  }

  @Override
  public void dropModel(String name) throws CatalogException {
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();

      pm.deletePersistent(getModel(name));

      tx.commit();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to drop model '" + name + "'", e);
    } finally {
        if (tx.isActive()) {
            tx.rollback();
        }
    }
  }

  @Override
  public void close() {
    pm.close();
  }
}
