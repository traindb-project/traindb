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

package traindb.schema;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public abstract class TrainDBSchema extends AbstractSchema {
  private final String name;
  private TrainDBDataSource dataSource;
  private ImmutableMap<String, Table> tableMap;

  public TrainDBSchema(String name, TrainDBDataSource dataSource) {
    this.name = name;
    this.dataSource = dataSource;
  }

  public final String getName() {
    return name;
  }

  @Override
  public final boolean isMutable() {
    return false;
  }

  @Override
  public final Map<String, Table> getTableMap() {
    return tableMap;
  }

  public final void setTableMap(ImmutableMap<String, Table> tableMap) {
    this.tableMap = tableMap;
  }

  public final TrainDBDataSource getDataSource() {
    return dataSource;
  }
}
