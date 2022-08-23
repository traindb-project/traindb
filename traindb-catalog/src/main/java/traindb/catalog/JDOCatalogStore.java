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

import java.util.Properties;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;
import traindb.common.TrainDBConfiguration;

public final class JDOCatalogStore implements CatalogStore {
  private static PersistenceManagerFactory pmf;

  @Override
  public void start(Properties conf) {
    Properties props = new Properties();
    props.setProperty("javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");

    props.setProperty("datanucleus.ConnectionDriverName",
        conf.getProperty(TrainDBConfiguration.CATALOG_STORE_PROPERTY_PREFIX + "driver",
                         "org.sqlite.JDBC"));
    props.setProperty("datanucleus.ConnectionURL",
        conf.getProperty(TrainDBConfiguration.CATALOG_STORE_PROPERTY_PREFIX + "uri",
                         "jdbc:sqlite://" + TrainDBConfiguration.getTrainDBPrefixPath()
                         + "/traindb_catalog_store.db"));
    props.setProperty("datanucleus.ConnectionUserName",
        conf.getProperty(TrainDBConfiguration.CATALOG_STORE_PROPERTY_PREFIX + "username", ""));
    props.setProperty("datanucleus.ConnectionPassword",
        conf.getProperty(TrainDBConfiguration.CATALOG_STORE_PROPERTY_PREFIX + "password", ""));

    props.setProperty("datanucleus.connectionPoolingType", "None");
    props.setProperty("datanucleus.schema.autoCreateAll", "true");

    pmf = JDOHelper.getPersistenceManagerFactory(props);
  }

  @Override
  public void stop() {
    pmf.close();
  }

  @Override
  public CatalogContext getCatalogContext() {
    return new JDOCatalogContext(pmf.getPersistenceManager());
  }
}
