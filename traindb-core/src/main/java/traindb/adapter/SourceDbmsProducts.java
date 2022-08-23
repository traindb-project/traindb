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

package traindb.adapter;

import java.io.FileReader;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;
import org.apache.calcite.util.Sources;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class SourceDbmsProducts {

  static Map<String, String> nameToDriverClass = null;
  static Map<String, String> nameToSqlDialectClass = null;
  static final String ADAPTER_EXTENSION_CONFIG_FILENAME = "adapter-ext-config.json";

  private static void loadDbmsAdapterConfiguration() {
    nameToDriverClass = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    nameToSqlDialectClass = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    nameToDriverClass.put("mysql", "com.mysql.cj.jdbc.Driver");
    nameToDriverClass.put("postgresql", "org.postgresql.Driver");

    JSONParser parser = new JSONParser();
    JSONArray adapterExtInfo = null;
    try {
      URL url = SourceDbmsProducts.class.getClassLoader().getResource(
          ADAPTER_EXTENSION_CONFIG_FILENAME);
      adapterExtInfo = (JSONArray) parser.parse(new FileReader(Sources.of(url).file()));

      for (Object obj : adapterExtInfo) {
        JSONObject infoObj = (JSONObject) obj;
        String protocol = (String) infoObj.get("jdbc.protocol");
        if (protocol == null) {
          continue;
        }
        String driverClass = (String) infoObj.get("jdbc.driver.class");
        nameToDriverClass.put(protocol, driverClass);
        String sqlDialectClass = (String) infoObj.get("sql.dialect.class");
        nameToSqlDialectClass.put(protocol, sqlDialectClass);
      }
    } catch (Exception e) {
      // ignore
    }
  }

  public static String getJdbcDriverClassName(String protocol) {
    if (nameToDriverClass == null) {
      loadDbmsAdapterConfiguration();
    }
    return nameToDriverClass.get(protocol);
  }

  public static String getSqlDialectClassName(String protocol) {
    if (nameToDriverClass == null) {
      loadDbmsAdapterConfiguration();
    }
    return nameToSqlDialectClass.get(protocol);
  }
}