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

package traindb.adapter;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;

public class KairosSqlDialect extends TrainDBSqlDialect {
  public static final Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
        .withIdentifierQuoteString("\"")
        .withUnquotedCasing(Casing.TO_LOWER)
        .withNullCollation(NullCollation.HIGH);
    DEFAULT = new KairosSqlDialect(DEFAULT_CONTEXT);
  }

  public KairosSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  public KairosSqlDialect(Context context) {
    super(context);
  }

  public SqlDialect getDefaultSqlDialect() {
    return DEFAULT;
  }

  @Override
  public boolean supportCatalogs() {
    return false;
  }
}
