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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import net.hydromatic.quidem.Quidem;
import org.apache.calcite.test.QuidemTest;
import org.apache.calcite.util.Sources;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Test that runs every Quidem file in the "core" module as a test.
 */
class TrainDBCoreQuidemTest extends QuidemTest {
  /**
   * Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   * <code>java TrainDBCoreQuidemTest sql/basic.iq</code>
   * </blockquote>
   */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new TrainDBCoreQuidemTest().test(arg);
    }
  }

  /**
   * For {@link QuidemTest#test(String)} parameters.
   */
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/basic.iq";
    return data(first);
  }

  @Override
  protected Quidem.ConnectionFactory createConnectionFactory() {
    JSONParser parser = new JSONParser();
    JSONArray testDbs = null;
    try {
      URL inUrl = this.getClass().getResource("/traindb-test-config.json");
      testDbs = (JSONArray) parser.parse(new FileReader(Sources.of(inUrl).file()));
    } catch (Exception e) {
      e.printStackTrace();
    }

    List<Quidem.ConnectionFactory> factories = new ArrayList<>();
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

}
