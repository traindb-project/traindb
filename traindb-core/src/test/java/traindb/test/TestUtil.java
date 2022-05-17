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

package traindb.test;

import java.io.FileReader;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import net.hydromatic.quidem.Quidem.ConnectionFactory;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TestUtil {

  private static final String TEST_CONFIG_FILE = "target/test-classes/traindb-test-config.json";

  public static ConnectionFactory createConnectionFactory() {
    JSONParser parser = new JSONParser();
    JSONArray testDbs = null;
    try {
      testDbs = (JSONArray) parser.parse(
          new FileReader(Paths.get(TEST_CONFIG_FILE).toAbsolutePath().toFile()));
    } catch (Exception e) {
      e.printStackTrace();
    }

    List<ConnectionFactory> factories = new ArrayList<>();
    for (Object obj : testDbs) {
      JSONObject testDb = (JSONObject) obj;
      String name = (String) testDb.get("name");
      String url = (String) testDb.get("url");
      String user = (String) testDb.get("user");
      String password = (String) testDb.get("password");
      factories.add(new SimpleConnectionFactory(name, url, user, password));
    }

    return new ChainingConnectionFactory(factories);
  }

  public static void executeIgnore(Statement stmt, String sql) {
    try {
      stmt.execute(sql);
    } catch (SQLException e) {
      /* ignore */
    }
  }

  public static void printResultSet(ResultSet rs) {
    ResultSetMetaData metadata = null;
    StringBuffer sb = new StringBuffer();
    System.out.println(Thread.currentThread().getStackTrace()[2].getMethodName());
    try {
      metadata = rs.getMetaData();
      int columnCount = metadata.getColumnCount();
      sb.append("| ");
      for (int i = 1; i <= columnCount; i++) {
        sb.append(metadata.getColumnName(i) + " | ");
      }
      int len = sb.length() - 1;
      System.out.println(StringUtils.repeat("-", len));
      System.out.println(sb.toString());
      System.out.println(StringUtils.repeat("-", len));

      while (rs.next()) {
        sb.setLength(0);
        sb.append("| ");
        for (int i = 1; i <= columnCount; i++) {
          sb.append(rs.getString(i) + " | ");
        }
        System.out.println(sb.toString());
      }
      System.out.println(StringUtils.repeat("-", len));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
