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

import java.sql.Connection;
import java.sql.DriverManager;
import net.hydromatic.quidem.Quidem;

/**
 * Connection factory that recognizes a single name,
 * and does not offer a reference connection.
 */
class SimpleConnectionFactory implements Quidem.ConnectionFactory {
  private final String name;
  private final String url;
  private final String user;
  private final String password;

  SimpleConnectionFactory(String name, String url, String user, String password) {
    this.name = name;
    this.url = url;
    this.user = user;
    this.password = password;
  }

  @Override
  public Connection connect(String name, boolean reference) throws Exception {
    if (!reference && name.equals(this.name)) {
      return DriverManager.getConnection(url, user, password);
    }
    return null;
  }
}