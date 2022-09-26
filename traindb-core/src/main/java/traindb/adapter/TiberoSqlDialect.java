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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TiberoSqlDialect extends TrainDBSqlDialect {
  public static final Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
        .withIdentifierQuoteString("\"")
        .withUnquotedCasing(Casing.TO_UPPER)
        .withNullCollation(NullCollation.HIGH);
    DEFAULT = new TiberoSqlDialect(DEFAULT_CONTEXT);
  }

  public TiberoSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  public TiberoSqlDialect(Context context) {
    super(context);
  }

  public SqlDialect getDefaultSqlDialect() {
    return DEFAULT;
  }

  @Override
  public boolean supportCatalogs() {
    return false;
  }

  @Override
  public @Nullable SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
      case SMALLINT:
        castSpec = "NUMBER(5)";
        break;
      case INTEGER:
        castSpec = "NUMBER(10)";
        break;
      case BIGINT:
        castSpec = "NUMBER(19)";
        break;
      case DOUBLE:
        castSpec = "DOUBLE PRECISION";
        break;
      default:
        return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }
}
