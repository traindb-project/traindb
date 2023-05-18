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

package traindb.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class TrainDBSpatialOperatorTable extends ReflectiveSqlOperatorTable {

  public static final SqlFunction ST_ASTEXT =
      new SqlFunction("st_astext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  private static @MonotonicNonNull TrainDBSpatialOperatorTable instance;

  public static synchronized TrainDBSpatialOperatorTable instance() {
    if (instance == null) {
      instance = new TrainDBSpatialOperatorTable();
      instance.init();
    }
    return instance;
  }

}
