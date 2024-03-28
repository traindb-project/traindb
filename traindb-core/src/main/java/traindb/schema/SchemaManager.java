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

package traindb.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.sql.DataSource;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.tools.Frameworks;
import org.apache.hadoop.service.AbstractService;
import traindb.adapter.TrainDBStandaloneSqlDialect;
import traindb.adapter.jdbc.TrainDBJdbcDataSource;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogStore;
import traindb.common.TrainDBLogger;


/**
 * Constructs in-memory schema from metadata in CatalogStore.
 */
public final class SchemaManager extends AbstractService {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(SchemaManager.class);
  private static SchemaManager singletonInstance;
  private final CatalogStore catalogStore;

  private final Map<String, TrainDBDataSource> dataSourceMap;
  private final Map<String, List<TrainDBSchema>> schemaMap;
  private final Map<String, List<TrainDBTable>> tableMap;

  // to synchronize requests for Calcite Schema
  private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();
  private SchemaPlus rootSchema;

  // for incremental query
  public List<String> saveQuery;
  public int saveQueryIdx;
  public List<List<Object>> totalRes;
  public List<String> header;
  public List<SqlAggFunction> aggCalls;

  private SchemaManager(CatalogStore catalogStore) {
    super(SchemaManager.class.getName());
    this.catalogStore = catalogStore;
    rootSchema = Frameworks.createRootSchema(false);
    dataSourceMap = new HashMap<>();
    schemaMap = new HashMap<>();
    tableMap = new HashMap<>();

    saveQuery = new ArrayList<>();
    totalRes = new ArrayList<>();
    header = new ArrayList<>();
    aggCalls = new ArrayList<>();
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

  public static SchemaManager getInstance(CatalogStore catalogStore) {
    if (singletonInstance == null) {
      assert catalogStore != null;
      singletonInstance = new SchemaManager(catalogStore);
    }
    return singletonInstance;
  }

  public void loadDataSource(DataSource dataSource) {
    SchemaPlus newRootSchema = Frameworks.createRootSchema(true);
    TrainDBCatalogDataSource catalogDataSource = new TrainDBCatalogDataSource(getCatalogContext());
    newRootSchema.add(catalogDataSource.getName(), catalogDataSource);
    addDataSourceToMaps(catalogDataSource);

    if (dataSource != null) {
      TrainDBJdbcDataSource newJdbcDataSource =
          new TrainDBJdbcDataSource(newRootSchema, dataSource);
      newRootSchema.add(newJdbcDataSource.getName(), newJdbcDataSource);
      addDataSourceToMaps(newJdbcDataSource);
    }

    writeLock.lock();
    this.rootSchema = newRootSchema;
    writeLock.unlock();
  }

  public void refreshDataSource() {
    DataSource dataSource = null;
    for (Map.Entry<String, TrainDBDataSource> entry : dataSourceMap.entrySet()) {
      if (entry.getValue() instanceof TrainDBJdbcDataSource) {
        dataSource = ((TrainDBJdbcDataSource) entry.getValue()).getDataSource();
        break;
      }
    }
    writeLock.lock();
    dataSourceMap.clear();
    schemaMap.clear();
    tableMap.clear();

    loadDataSource(dataSource);
    writeLock.unlock();
  }

  public TrainDBDataSource getJdbcDataSource() {
    return dataSourceMap.get("jdbc");
  }

  private void addDataSourceToMaps(TrainDBDataSource traindbDataSource) {
    dataSourceMap.put(traindbDataSource.getName(), traindbDataSource);
    for (Schema schema : traindbDataSource.getSubSchemaMap().values()) {
      TrainDBSchema traindbSchema = (TrainDBSchema) schema;
      addToListMap(schemaMap, traindbSchema.getName(), traindbSchema);
      for (Table table : traindbSchema.getTableMap().values()) {
        TrainDBTable traindbTable = (TrainDBTable) table;
        addToListMap(tableMap, traindbTable.getName(), traindbTable);
      }
    }
  }

  private <T> void addToListMap(Map<String, List<T>> map, String key, T value) {
    List<T> values = map.get(key);
    if (values == null) {
      values = new ArrayList<>();
      map.put(key, values);
    }
    values.add(value);
  }

  public CatalogContext getCatalogContext() {
    return catalogStore.getCatalogContext();
  }

  public SchemaPlus getCurrentSchema() {
    SchemaPlus currentSchema;
    readLock.lock();
    currentSchema = rootSchema;
    readLock.unlock();
    return currentSchema;
  }

  public SqlDialect getDialect() {
    for (Map.Entry<String, TrainDBDataSource> entry : dataSourceMap.entrySet()) {
      if (entry.getValue() instanceof TrainDBJdbcDataSource) {
        return ((TrainDBJdbcDataSource) entry.getValue()).getDialect();
      }
    }
    return new TrainDBStandaloneSqlDialect();
  }

  public TrainDBTable getTable(String schemaName, String tableName) {
    List<TrainDBSchema> schemaList = schemaMap.get(schemaName);
    if (schemaList == null) {
      return null;
    }
    for (int i = 0; i < schemaList.size(); i++) {
      TrainDBSchema schema = schemaList.get(i);
      TrainDBTable table = (TrainDBTable) schema.getTable(tableName);
      if (table != null) {
        return table;
      }
    }
    return null;
  }

  /*
   * lockRead()/unlockRead() are used to protect rootSchema returned by
   * getCurrentSchema() because we don't know how long the schema will be used
   */
  public void lockRead() {
    readLock.lock();
  }

  public void unlockRead() {
    readLock.unlock();
  }
}
