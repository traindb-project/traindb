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

package traindb.test.spt;

import java.util.Collection;
import org.apache.calcite.test.QuidemTest;
import traindb.test.TrainDBCoreQuidemTest;

/**
 * Test that runs every Quidem file in the "core" module as a test.
 */
class TrainDBKairosSpatialQuidemTest extends TrainDBCoreQuidemTest {
  /**
   * Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   * <code>java TrainDBSpatialQuidemTest sql/spt/kairos.iq</code>
   * </blockquote>
   */
  public static void main(String[] args) throws Exception {
    Class.forName("kr.co.realtimetech.kairos.jdbc.kairosDriver");
    for (String arg : args) {
      new TrainDBKairosSpatialQuidemTest().test(arg);
    }
  }

  /**
   * For {@link QuidemTest#test(String)} parameters.
   */
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/spt/kairos.iq";
    return data(first);
  }

}
