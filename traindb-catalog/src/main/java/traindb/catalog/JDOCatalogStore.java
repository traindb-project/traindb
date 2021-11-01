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
import traindb.common.TrainDBLogger;

public final class JDOCatalogStore implements CatalogStore {

  private static PersistenceManagerFactory pmf;
  private final TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());

  @Override
  public void start(Properties conf) throws CatalogException {
    Properties props = new Properties();
    props.setProperty("javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
    props.setProperty("datanucleus.ConnectionDriverName",
        conf.getProperty("traindb.metastore.driver", "org.sqlite.JDBC"));
    props.setProperty("datanucleus.ConnectionURL",
        conf.getProperty("traindb.metastore.uri", "jdbc:sqlite://"
            + System.getenv("TRAINDB_PREFIX").trim() + "/traindb_metastore.db"));
    props.setProperty(
        "datanucleus.ConnectionUserName", conf.getProperty("traindb.metastore.username", ""));
    props.setProperty(
        "datanucleus.ConnectionPassword", conf.getProperty("traindb.metastore.password", ""));
    // FIXME implement connection pooling
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
