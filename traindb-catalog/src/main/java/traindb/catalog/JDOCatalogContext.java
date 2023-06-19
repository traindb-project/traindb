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

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.catalog.pm.MColumn;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MModeltype;
import traindb.catalog.pm.MQueryLog;
import traindb.catalog.pm.MSchema;
import traindb.catalog.pm.MSynopsis;
import traindb.catalog.pm.MTable;
import traindb.catalog.pm.MTask;
import traindb.common.TrainDBLogger;

public final class JDOCatalogContext implements CatalogContext {

  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(JDOCatalogContext.class);
  private final PersistenceManager pm;

  public JDOCatalogContext(PersistenceManager persistenceManager) {
    pm = persistenceManager;
  }

  @Override
  public boolean modeltypeExists(String name) {
    return getModeltype(name) != null;
  }

  @Override
  public @Nullable MModeltype getModeltype(String name) {
    try {
      Query query = pm.newQuery(MModeltype.class);
      setFilterPatterns(query, ImmutableMap.of("modeltype_name", name));
      query.setUnique(true);

      return (MModeltype) query.execute(name);
    } catch (RuntimeException e) {
    }
    return null;
  }

  @Override
  public Collection<MModeltype> getModeltypes() throws CatalogException {
    return getModeltypes(ImmutableMap.of());
  }

  @Override
  public Collection<MModeltype> getModeltypes(Map<String, Object> filterPatterns)
      throws CatalogException {
    try {
      Query query = pm.newQuery(MModeltype.class);
      setFilterPatterns(query, filterPatterns);
      return (List<MModeltype>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get modeltypes", e);
    }
  }

  @Override
  public MModeltype createModeltype(String name, String type, String location, String className,
                                    String uri, String hyperparameters) throws CatalogException {
    try {
      MModeltype mModeltype = new MModeltype(name, type, location, className, uri, hyperparameters);
      pm.makePersistent(mModeltype);
      return mModeltype;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to create modeltype '" + name + "'", e);
    }
  }

  @Override
  public void dropModeltype(String name) throws CatalogException {
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();

      pm.deletePersistent(getModeltype(name));

      tx.commit();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to drop modeltype '" + name + "'", e);
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  @Override
  public MModel trainModel(
      String modeltypeName, String modelName, String schemaName, String tableName,
      List<String> columnNames, RelDataType dataType, @Nullable Long baseTableRows,
      @Nullable Long trainedRows, @Nullable String options) throws CatalogException {
    try {
      MSchema mSchema = getSchema(schemaName);
      if (mSchema == null) {
        mSchema = new MSchema(schemaName);
        pm.makePersistent(mSchema);
      }

      MTable mTable = getTable(schemaName, tableName);
      if (mTable == null) {
        mTable = new MTable(tableName, "TABLE", mSchema);
        pm.makePersistent(mTable);
      }

      List<RelDataTypeField> fields = dataType.getFieldList();
      for (int i = 0; i < dataType.getFieldCount(); i++) {
        RelDataTypeField field = fields.get(i);
        MColumn mColumn = new MColumn(field.getName(),
            field.getType().getSqlTypeName().getJdbcOrdinal(),
            field.getType().getPrecision(),
            field.getType().getScale(),
            field.getType().isNullable(),
            mTable);

        pm.makePersistent(mColumn);
      }

      MModel mModel = new MModel(
          getModeltype(modeltypeName), modelName, schemaName, tableName, columnNames,
          baseTableRows, trainedRows, options == null ? "" : options);
      pm.makePersistent(mModel);
      return mModel;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to train model '" + modelName + "'", e);
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
  public Collection<MModel> getModels() throws CatalogException {
    return getModels(ImmutableMap.of());
  }

  @Override
  public Collection<MModel> getModels(Map<String, Object> filterPatterns) throws CatalogException {
    try {
      Query query = pm.newQuery(MModel.class);
      setFilterPatterns(query, filterPatterns);
      return (List<MModel>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get models", e);
    }
  }

  @Override
  public Collection<MModel> getInferenceModels(String baseSchema, String baseTable)
      throws CatalogException {
    return getModels(ImmutableMap.of(
        "schema_name", baseSchema, "table_name", baseTable, "modeltype.category", "INFERENCE"));
  }

  @Override
  public boolean modelExists(String name) {
    return getModel(name) != null;
  }

  @Override
  public @Nullable MModel getModel(String name) {
    try {
      Query query = pm.newQuery(MModel.class);
      setFilterPatterns(query, ImmutableMap.of("model_name", name));
      query.setUnique(true);

      return (MModel) query.execute(name);
    } catch (RuntimeException e) {
    }
    return null;
  }

  @Override
  public MSynopsis createSynopsis(String synopsisName, String modelName, Integer rows,
                                  @Nullable Double ratio) throws CatalogException {
    try {
      MSynopsis mSynopsis = new MSynopsis(synopsisName, rows, ratio, getModel(modelName));
      pm.makePersistent(mSynopsis);
      return mSynopsis;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to create synopsis '" + synopsisName + "'", e);
    }
  }

  @Override
  public Collection<MSynopsis> getAllSynopses() throws CatalogException {
    return getAllSynopses(ImmutableMap.of());
  }

  @Override
  public Collection<MSynopsis> getAllSynopses(String baseSchema, String baseTable)
      throws CatalogException {
    return getAllSynopses(ImmutableMap.of(
        "model.schema_name", baseSchema, "model.table_name", baseTable));
  }

  @Override
  public Collection<MSynopsis> getAllSynopses(Map<String, Object> filterPatterns)
      throws CatalogException {
    try {
      Query query = pm.newQuery(MSynopsis.class);
      setFilterPatterns(query, filterPatterns);
      return (List<MSynopsis>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get synopses", e);
    }
  }

  @Override
  public boolean synopsisExists(String name) {
    return getSynopsis(name) != null;
  }

  @Override
  public @Nullable MSynopsis getSynopsis(String name) {
    try {
      Query query = pm.newQuery(MSynopsis.class);
      setFilterPatterns(query, ImmutableMap.of("synopsis_name", name));
      query.setUnique(true);

      return (MSynopsis) query.execute(name);
    } catch (RuntimeException e) {
    }
    return null;
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
  public @Nullable MSchema getSchema(String name) {
    try {
      Query query = pm.newQuery(MSchema.class);
      setFilterPatterns(query, ImmutableMap.of("schema_name", name));
      query.setUnique(true);

      return (MSchema) query.execute(name);
    } catch (RuntimeException e) {
    }
    return null;
  }

  @Override
  public @Nullable MTable getTable(String schemaName, String tableName) {
    try {
      Query query = pm.newQuery(MTable.class);
      setFilterPatterns(query,
          ImmutableMap.of("table_name", tableName, "schema.schema_name", schemaName));
      query.setUnique(true);

      return (MTable) query.execute(tableName, schemaName);
    } catch (RuntimeException e) {
    }
    return null;
  }

  @Override
  public Collection<MQueryLog> getQueryLogs() throws CatalogException {
    return getQueryLogs(ImmutableMap.of());
  }

  @Override
  public Collection<MQueryLog> getQueryLogs(Map<String, Object> filterPatterns)
      throws CatalogException {
    try {
      Query query = pm.newQuery(MQueryLog.class);
      setFilterPatterns(query, filterPatterns);
      return (List<MQueryLog>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get query log", e);
    }
  }

  @Override
  public MQueryLog insertQueryLog(String start, String user, String query) throws CatalogException {
    try {
      MQueryLog mQuery = new MQueryLog(start, user, query);
      pm.makePersistent(mQuery);
      return mQuery;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to create query log '" + query + "'", e);
    }
  }

  @Override
  public void deleteQueryLogs(Integer cnt) throws CatalogException {
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();

      Query query = pm.newQuery(MQueryLog.class);

      if (cnt > 0) {
        query.range(0, cnt);
      }

      Collection<MTask> ret = (List<MTask>) query.execute();
      pm.deletePersistentAll(ret);

      //pm.deletePersistent(getQueryLog());
      tx.commit();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to delete query log", e);
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  @Override
  public Collection<MTask> getTaskLogs() throws CatalogException {
    return getTaskLogs(ImmutableMap.of());
  }

  @Override
  public Collection<MTask> getTaskLogs(Map<String, Object> filterPatterns) throws CatalogException {
    try {
      Query query = pm.newQuery(MTask.class);
      setFilterPatterns(query, filterPatterns);
      return (List<MTask>) query.execute();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to get task log", e);
    }
  }

  @Override
  public MTask insertTask(String time, Integer idx, String task, String status)
      throws CatalogException {
    try {
      MTask mtask = new MTask(time, idx, task, status);
      pm.makePersistent(mtask);
      return mtask;
    } catch (RuntimeException e) {
      throw new CatalogException("failed to create task log '" + task + "'", e);
    }
  }

  @Override
  public void deleteTasks(Integer cnt) throws CatalogException {
    Transaction tx = pm.currentTransaction();
    try {
      tx.begin();

      Query query = pm.newQuery(MTask.class);

      if (cnt > 0) {
        query.range(0, cnt);
      }

      Collection<MTask> ret = (List<MTask>) query.execute();

      pm.deletePersistentAll(ret);
      //pm.deletePersistent(getTaskLog());

      tx.commit();
    } catch (RuntimeException e) {
      throw new CatalogException("failed to delete task log", e);
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

  private void setFilterPatterns(Query query, Map<String, Object> filterPatterns) {
    for (Map.Entry<String, Object> entry : filterPatterns.entrySet()) {
      Object v = entry.getValue();
      if (v instanceof String) {
        query.setFilter(entry.getKey() + ".matches('" + v + "')");
      } else {
        query.setFilter(entry.getKey() + " == " + v + " ");
      }
    }
  }
}
