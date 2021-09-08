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

package traindb.jdbc;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class Driver extends org.verdictdb.jdbc41.Driver {

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      System.err.println("Error occurred while registering TrainDB driver:");
      System.err.println(e.getMessage());
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (acceptsURL(url)) {
      String newUrl = url;
      try {
        String[] tokens = url.split(":");
        if (tokens.length >= 2 && (tokens[1].equalsIgnoreCase("traindb"))) {
          List<String> newTokens = new ArrayList<>();
          for (int i = 0; i < tokens.length; ++i) {
            if (i != 1) {
              newTokens.add(tokens[i]);
            }
          }
          newUrl = Joiner.on(":").join(newTokens);
        }
        Connection verdictConnection = new TrainDBConnection(newUrl, info);
        return verdictConnection;
      } catch (Exception e) {
        e.printStackTrace();
        throw new SQLException(e.getMessage());
      }
    }
    return null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    if (url.startsWith("jdbc:traindb:")) {
      return true;
    }
    return false;
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }
}
