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

package traindb.adapter.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Util;

/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor extends RelToSqlConverter {
  public JdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    super(dialect);
    Util.discard(typeFactory);
  }

  public Result implement(RelNode node) {
    return dispatch(node);
  }
}
