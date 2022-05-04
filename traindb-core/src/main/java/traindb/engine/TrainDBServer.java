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

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import traindb.catalog.CatalogStore;
import traindb.catalog.JDOCatalogStore;
import traindb.common.TrainDBConfiguration;
import traindb.common.TrainDBLogger;
import traindb.schema.SchemaManager;


public class TrainDBServer extends CompositeService {
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBServer.class);

  public TrainDBServer() {
    super(TrainDBServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    CatalogStore catalogStore = new JDOCatalogStore();
    Properties catProps = new Properties();
    catProps.putAll(conf.getPropsWithPrefix(TrainDBConfiguration.CATALOG_STORE_PROPERTY_PREFIX));
    catalogStore.start(catProps);
    CatalogService catalogService = new CatalogService(catalogStore);
    addService(catalogService);

    SchemaManager schemaManager = SchemaManager.getInstance(catalogStore);
    addService(schemaManager);

    SessionFactory sessFactory = new SessionFactory(catalogStore, schemaManager);
    SessionServer sessServer = new SessionServer(sessFactory);
    addService(sessServer);

    super.serviceInit(conf);
  }

  @Override
  public String getName() {
    return "TrainDBServer";
  }

  private void initAndStart(Configuration conf) {
    CompositeServiceShutdownHook hook = new CompositeServiceShutdownHook(this);
    ShutdownHookManager.get().addShutdownHook(hook, SHUTDOWN_HOOK_PRIORITY);

    init(conf);
    start();
  }

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(TrainDBServer.class, args, LOG);
    TrainDBServer master = new TrainDBServer();

    TrainDBConfiguration conf = new TrainDBConfiguration(new Properties());
    conf.loadConfiguration();
    master.initAndStart(conf.asHadoopConfiguration());
  }
}
