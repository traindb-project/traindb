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

import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import traindb.catalog.CatalogStore;
import traindb.common.TrainDBLogger;

public class CatalogService extends AbstractService {
  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(CatalogService.class);

  private final Properties props = new Properties();
  private final CatalogStore catalogStore;

  public CatalogService(CatalogStore catalogStore) {
    super(catalogStore.getClass().getName());
    this.catalogStore = catalogStore;
  }

  @Override
  protected final void serviceInit(Configuration conf) throws Exception {
    LOG.info("initialize service - " + getName());
    props.clear();
    for (Map.Entry<String, String> e : conf) {
      String key = e.getKey();
      if (key.startsWith("catalog.store.")) {
        props.put(key, e.getValue());
      }
    }
    super.serviceInit(conf);
  }

  @Override
  protected final void serviceStart() throws Exception {
    LOG.info("start service - " + getName());
    catalogStore.start(props);
    super.serviceStart();
  }

  @Override
  protected final void serviceStop() throws Exception {
    LOG.info("stop service - " + getName());
    catalogStore.stop();
    super.serviceStop();
  }
}
