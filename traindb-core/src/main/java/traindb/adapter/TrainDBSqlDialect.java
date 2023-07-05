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
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class TrainDBSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_LOWER);

  public TrainDBSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  public TrainDBSqlDialect(SqlDialect.Context context) {
    super(context);
  }

  public abstract SqlDialect getDefaultSqlDialect();

  public boolean supportCatalogs() {
    return true;
  }

  public boolean supportCreateTableAsSelect() {
    return true;
  }

  @Override
  public @Nullable Quoting getQuoting() {
    return super.getQuoting();
  }

  public boolean useCustomBinaryString() {
    return false;
  }


  public SqlBinaryStringLiteral convertToBinaryString(byte[] binaryValue, SqlParserPos POS) {
    return SqlCharStringLiteral.createBinaryString(binaryValue, POS);
  }
}
