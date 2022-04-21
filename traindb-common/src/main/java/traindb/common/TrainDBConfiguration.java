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

package traindb.common;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;

public class TrainDBConfiguration extends CalciteConnectionConfigImpl {
  private final TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());

  private final String TRAINDB_CONFIG_FILENAME = "traindb.properties";
  private Properties props;

  public TrainDBConfiguration(Properties p) {
    super(p);
    this.props = p;
  }

  public void loadConfiguration() {
    try {
      loadConfigurationFile(props, TRAINDB_CONFIG_FILENAME);
    } catch (Exception e) {
      LOG.debug("Could not load configuration file.");
    }
  }

  private void loadConfigurationFile(Properties props, String filename) throws Exception {
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(filename);
    props.load(inputStream);
  }

  public Properties getProps() {
    return props;
  }

  public String getModelRunnerPath() {
    return getTrainDBPrefixPath() + "/models/TrainDBModelRunner.py";
  }

  public static String getTrainDBPrefixPath() {
    String prefix = System.getProperty("TRAINDB_PREFIX");
    if (prefix == null) {
      prefix = System.getenv("TRAINDB_PREFIX");
    }
    return prefix.trim();
  }

  public static String absoluteUri(String uri) {
    File f = new File(uri);
    if (!f.isAbsolute()) {
      return getTrainDBPrefixPath() + "/" + uri;
    }
    return uri;
  }

  @Override
  public Casing unquotedCasing() {
    return Casing.TO_LOWER;
  }
}
