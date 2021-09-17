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

import com.rits.cloning.Cloner;
import java.io.InputStream;
import java.util.Properties;
import org.verdictdb.commons.VerdictOption;

public class TrainDBConfiguration extends VerdictOption {

  private final TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());

  private final String TRAINDB_CONFIG_FILENAME = "traindb.properties";
  private Properties props;

  public TrainDBConfiguration() {
    super();
    this.props = new Properties();
  }

  public void loadConfiguration() {
    try {
      loadConfigurationFile(props, TRAINDB_CONFIG_FILENAME);
    } catch (Exception e) {
      LOG.debug("Could not load configuration file.");
    }

    if (LOG.isDebugEnabled()) {
      for (Object key : props.keySet()) {
        LOG.debug(key + "=" + props.getProperty(key.toString()));
      }
    }
  }

  private void loadConfigurationFile(Properties props, String filename) throws Exception {
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(filename);
    props.load(inputStream);
  }

  public Properties getProps() {
    return props;
  }

  @Override
  public TrainDBConfiguration copy() {
    // FIXME Cloner is not working in Java 9+
    return new Cloner().deepClone(this);
  }
}
