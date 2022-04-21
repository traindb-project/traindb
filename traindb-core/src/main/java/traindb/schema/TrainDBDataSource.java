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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;

public abstract class TrainDBDataSource extends AbstractSchema {

  private final String name;
  private ImmutableMap<String, Schema> subSchemaMap;

  public TrainDBDataSource() {
    this.name = "traindb"; // FIXME
  }

  public final String getName() {
    return name;
  }

  @Override
  public final boolean isMutable() {
    return false;
  }

  @Override
  public final Map<String, Schema> getSubSchemaMap() {
    return subSchemaMap;
  }

  public final void setSubSchemaMap(ImmutableMap<String, Schema> subSchemaMap) {
    this.subSchemaMap = subSchemaMap;
  }
}
