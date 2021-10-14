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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModelInstance;
import traindb.catalog.pm.MSynopsis;
import traindb.common.TrainDBLogger;

public final class JDOCatalogContext implements CatalogContext {

  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(JDOCatalogContext.class);
  private final PersistenceManager pm;

  public JDOCatalogContext(PersistenceManager persistenceManager) {
    pm = persistenceManager;
  }

  @Override
  public boolean modelExists(String name) throws CatalogException {
    return getModel(name) != null;
  }

  @Override
  public MModel getModel(String name) throws CatalogException {
    try {
      Query query = pm.newQuery(MModel.class);
      query.setFilter("name == modelName");
      query.declareParameters("String modelName");
      query.setUnique(true);

      return (MModel) query.execute(name);
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get model '" + name + "'", e);
    }
  }

  @Override
  public Collection<MModel> getModels() throws CatalogException {
    try {
      Query query = pm.newQuery(MModel.class);
      return (List<MModel>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get models", e);
    }
  }

  @Override
  public MModel createModel(String name, String type, String location, String className, String uri)
      throws CatalogException {
    try {
      MModel mModel = new MModel(name, type, location, className, uri);
      pm.makePersistent(mModel);
      return mModel;
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new CatalogException("failed to create model '" + name + "'", e);
    }

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
  public MModelInstance trainModelInstance(
      String modelName, String modelInstanceName, String schemaName, String tableName,
      List<String> columnNames) throws CatalogException {
    try {
      MModelInstance mModelInstance = new MModelInstance(
        getModel(modelName), modelInstanceName, schemaName, tableName, columnNames);
      pm.makePersistent(mModelInstance);
      return mModelInstance;
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new CatalogException("failed to train model instance '" + modelInstanceName + "'", e);
    }
  }

  @Override
  public void dropModelInstance(String name) throws CatalogException {
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();

      pm.deletePersistent(getModelInstance(name));

      tx.commit();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to drop model instance '" + name + "'", e);
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  @Override
  public Collection<MModelInstance> getModelInstances(String modelName) throws CatalogException {
    try {
      Query query = pm.newQuery(MModelInstance.class);
      query.setFilter("model.name == modelName");
      query.declareParameters("String modelName");

      return (List<MModelInstance>) query.execute(modelName);
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get model '" + modelName + "' instances", e);
    }
  }

  @Override
  public boolean modelInstanceExists(String name) throws CatalogException {
    return getModelInstance(name) != null;
  }

  @Override
  public MModelInstance getModelInstance(String name) throws CatalogException {
    try {
      Query query = pm.newQuery(MModelInstance.class);
      query.setFilter("name == name");
      query.declareParameters("String name");
      query.setUnique(true);

      return (MModelInstance) query.execute(name);
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get model instance '" + name + "'", e);
    }
  }

  @Override
  public Path getModelInstancePath(String modelName, String modelInstanceName) {
    return Paths.get(System.getenv("TRAINDB_PREFIX").trim(), "models",
                     modelName, modelInstanceName);
  }

  @Override
  public MSynopsis createSynopsis(String synopsisName, String modelInstanceName)
      throws CatalogException {
    try {
      MSynopsis mSynopsis = new MSynopsis(synopsisName, getModelInstance(modelInstanceName));
      pm.makePersistent(mSynopsis);
      return mSynopsis;
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new CatalogException("failed to create synopsis '" + synopsisName + "'", e);
    }
  }

  @Override
  public void close() {
    pm.close();
  }
}
