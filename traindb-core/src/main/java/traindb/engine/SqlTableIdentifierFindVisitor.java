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

package traindb.engine;

import java.util.ArrayList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public final class SqlTableIdentifierFindVisitor extends SqlBasicVisitor<SqlNode> {
  private final ArrayList<SqlIdentifier> tableIds;

  public SqlTableIdentifierFindVisitor(ArrayList<SqlIdentifier> tableIds) {
    this.tableIds = tableIds;
  }

  private void flatten(SqlNode node, ArrayList<SqlNode> list) {
    switch (node.getKind()) {
      case JOIN:
        SqlJoin join = (SqlJoin) node;
        flatten(join.getLeft(), list);
        flatten(join.getRight(), list);
        break;
      case AS:
        SqlCall call = (SqlCall) node;
        flatten(call.operand(0), list);
        break;
      case IDENTIFIER:
        tableIds.add((SqlIdentifier) node);
        break;
      default:
        break;
    }
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      SqlNode from = ((SqlSelect) call).getFrom();
      ArrayList<SqlNode> list = new ArrayList<>();
      flatten(from, list);
      return null;
    }

    return super.visit(call);
  }

}
