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
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModelInstance;
import traindb.catalog.pm.MSynopsis;
import traindb.common.TrainDBConfiguration;
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
      List<String> columnNames, @Nullable Long baseTableRows, @Nullable Long trainedRows)
      throws CatalogException {
    try {
      MModelInstance mModelInstance = new MModelInstance(
          getModel(modelName), modelInstanceName, schemaName, tableName, columnNames,
          baseTableRows, trainedRows);
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
  public Collection<MModelInstance> getModelInstances() throws CatalogException {
    try {
      Query query = pm.newQuery(MModelInstance.class);
      return (List<MModelInstance>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get model instances", e);
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
      query.setFilter("name == modelInstanceName");
      query.declareParameters("String modelInstanceName");
      query.setUnique(true);

      return (MModelInstance) query.execute(name);
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get model instance '" + name + "'", e);
    }
  }

  @Override
  public Path getModelInstancePath(String modelName, String modelInstanceName) {
    return Paths.get(TrainDBConfiguration.getTrainDBPrefixPath(), "models",
                     modelName, modelInstanceName);
  }

  @Override
  public MSynopsis createSynopsis(String synopsisName, String modelInstanceName, Integer rows,
                                  @Nullable Double ratio) throws CatalogException {
    try {
      MSynopsis mSynopsis = new MSynopsis(
          synopsisName, rows, ratio, getModelInstance(modelInstanceName));
      pm.makePersistent(mSynopsis);
      return mSynopsis;
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new CatalogException("failed to create synopsis '" + synopsisName + "'", e);
    }
  }

  @Override
  public Collection<MSynopsis> getAllSynopses() throws CatalogException {
    try {
      Query query = pm.newQuery(MSynopsis.class);
      return (List<MSynopsis>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get synopses", e);
    }
  }

  @Override
  public Collection<MSynopsis> getAllSynopses(String baseSchema, String baseTable)
      throws CatalogException {
    try {
      Query query = pm.newQuery(MSynopsis.class);
      query.setFilter(
          "modelInstance.schemaName == baseSchema && modelInstance.tableName == baseTable");
      query.declareParameters("String baseSchema, String baseTable");
      Collection<MSynopsis> ret = (List<MSynopsis>) query.execute(baseSchema, baseTable);
      return ret;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get synopses", e);
    }
  }

  @Override
  public boolean synopsisExists(String name) throws CatalogException {
    return getSynopsis(name) != null;
  }

  @Override
  public MSynopsis getSynopsis(String name) throws CatalogException {
    try {
      Query query = pm.newQuery(MSynopsis.class);
      query.setFilter("name == synopsisName");
      query.declareParameters("String synopsisName");
      query.setUnique(true);

      return (MSynopsis) query.execute(name);
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get synopsis '" + name + "'", e);
    }
  }

  @Override
  public void dropSynopsis(String name) throws CatalogException {
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();

      pm.deletePersistent(getSynopsis(name));

      tx.commit();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to drop synopsis '" + name + "'", e);
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
