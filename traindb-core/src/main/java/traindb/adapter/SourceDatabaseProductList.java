/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package traindb.adapter;

import java.util.Map;
import java.util.TreeMap;

public class SourceDatabaseProductList {

  static Map<String, String> nameToDriverClass = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

  static {
    nameToDriverClass.put("mysql", "com.mysql.cj.jdbc.Driver");
    nameToDriverClass.put("postgresql", "org.postgresql.Driver");
    nameToDriverClass.put("kairos", "org.postgresql.Driver");
    nameToDriverClass.put("tibero", "com.tmax.tibero.jdbc.Driver");
  }

  public static String getJdbcDriverClassName(String protocol) {
    return nameToDriverClass.get(protocol);
  }
}