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
import java.util.Stack;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public final class SqlTableIdentifierFindVisitor extends SqlBasicVisitor<SqlNode> {
  private final Stack<State> nodeStack = new Stack<>();
  private final ArrayList<SqlIdentifier> tableIds;

  /*
   * To find table name identifiers, we use a state stack.
   * It is used to indicate whether an identifier is in FROM clause.
   */
  private enum State {
    NOT_FROM,
    FROM
  }

  public SqlTableIdentifierFindVisitor(ArrayList<SqlIdentifier> tableIds) {
    this.tableIds = tableIds;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      int i = 0;
      for (SqlNode operand : call.getOperandList()) {
        // FROM operand
          if (i == 2) {
              nodeStack.push(State.FROM);
          } else {
              nodeStack.push(State.NOT_FROM);
          }

        i++;

          if (operand == null) {
              continue;
          }

        operand.accept(this);
        nodeStack.pop();
      }
      return null;
    }

    SqlOperator operator = call.getOperator();
    if (operator != null && operator.getKind() == SqlKind.AS) {
      // AS operator will be probed only if it is in FROM clause
        if (nodeStack.peek() == State.FROM) {
            call.operand(0).accept(this);
        }
      return null;
    }

    return super.visit(call);
  }

  @Override
  public SqlNode visit(SqlIdentifier identifier) {
    // check whether this is fully qualified table name
      if (!nodeStack.empty() && nodeStack.peek() == State.FROM) {
          tableIds.add(identifier);
      }

    return identifier;
  }
}
