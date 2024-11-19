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

import static org.apache.calcite.util.Static.RESOURCE;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.BitString;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.sql.SqlZeroXBinaryStringLiteral;

public class KairosSqlDialect extends TrainDBSqlDialect {
  public static final Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
        .withIdentifierQuoteString("")
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

  @Override
  public boolean supportCreateTableAsSelect() {
    return false;
  }

  @Override
  public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
                                 @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  public boolean useCustomBinaryString() {
    return true;
  }

  public SqlBinaryStringLiteral convertToBinaryString(byte[] value, SqlParserPos pos) {
    BitString bits;
    try {
      bits = BitString.createFromBytes(value);
    } catch (NumberFormatException e) {
      throw SqlUtil.newContextException(pos, RESOURCE.binaryLiteralInvalid());
    }
    return new SqlZeroXBinaryStringLiteral(bits, pos);
  }
}
