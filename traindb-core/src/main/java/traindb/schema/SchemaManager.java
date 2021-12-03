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

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.sql.DataSource;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import traindb.catalog.CatalogStore;
import traindb.common.TrainDBLogger;


/**
 * Constructs in-memory schema from metadata in CatalogStore.
 */
public final class SchemaManager {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(SchemaManager.class);
  private static SchemaManager singletonInstance;
  private final CatalogStore catalogStore;
  private TrainDBJdbcDataSource traindbDataSource;

  // to synchronize requests for Calcite Schema
  private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();
  private SchemaPlus rootSchema;

  private SchemaManager(CatalogStore catalogStore) {
    this.catalogStore = catalogStore;
    rootSchema = Frameworks.createRootSchema(false);
    traindbDataSource = null;
  }

  public static SchemaManager getInstance(CatalogStore catalogStore) {
    if (singletonInstance == null) {
      assert catalogStore != null;
      singletonInstance = new SchemaManager(catalogStore);
    }
    return singletonInstance;
  }

  public void loadDataSource(DataSource dataSource) {
    SchemaPlus newRootSchema = Frameworks.createRootSchema(false);
    TrainDBJdbcDataSource newJdbcDataSource = new TrainDBJdbcDataSource(newRootSchema, dataSource);
    addSchemaInfo(newRootSchema, newJdbcDataSource.getSubSchemaMap());

    writeLock.lock();
    this.traindbDataSource = newJdbcDataSource;
    this.rootSchema = newRootSchema;
    writeLock.unlock();
  }

  public void refreshDataSource() {
    loadDataSource(traindbDataSource.getDataSource());
  }

  private void addSchemaInfo(SchemaPlus parentSchema,  Map<String, Schema> subSchemaMap) {
    for (Map.Entry<String, Schema> entry : subSchemaMap.entrySet()) {
      TrainDBJdbcSchema schema = (TrainDBJdbcSchema) entry.getValue();
      parentSchema.add(entry.getKey(), schema);
    }
  }

  public SchemaPlus getCurrentSchema() {
    SchemaPlus currentSchema;
    readLock.lock();
    currentSchema = rootSchema;
    readLock.unlock();
    return currentSchema;
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
