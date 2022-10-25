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

package traindb.adapter.python;

import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSqlConvertletTable;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PythonRexToSqlNodeConverterImpl extends RexToSqlNodeConverterImpl {

  private List<String> inputColumns;

  public PythonRexToSqlNodeConverterImpl(RexSqlConvertletTable convertletTable) {
    super(convertletTable);
    this.inputColumns = null;
  }

  public void setInputcolumns(List<String> inputColumns) {
    this.inputColumns = inputColumns;
  }

  @Override
  public @Nullable SqlNode convertNode(RexNode node) {
    if (node instanceof RexLiteral) {
      return convertLiteral((RexLiteral) node);
    } else if (node instanceof RexInputRef) {
      return convertInputRef((RexInputRef) node, inputColumns);
    } else if (node instanceof RexCall) {
      return convertCall((RexCall) node);
    }
    return null;
  }

  public @Nullable SqlNode convertInputRef(RexInputRef ref, @Nullable List<String> inputColumns) {
    if (inputColumns == null) {
      return convertInputRef(ref);
    }
    String column = inputColumns.get(ref.getIndex());
    return new SqlIdentifier(column, SqlParserPos.ZERO);
  }
}
