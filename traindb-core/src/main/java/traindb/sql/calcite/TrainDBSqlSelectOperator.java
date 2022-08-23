/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.sql.calcite;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An operator describing a query. (Not a query itself.)
 *
 * <p>Operands are:</p>
 *
 * <ul>
 * <li>0: distinct ({@link SqlLiteral})</li>
 * <li>1: selectClause ({@link SqlNodeList})</li>
 * <li>2: fromClause ({@link SqlCall} to "join" operator)</li>
 * <li>3: whereClause ({@link SqlNode})</li>
 * <li>4: havingClause ({@link SqlNode})</li>
 * <li>5: groupClause ({@link SqlNode})</li>
 * <li>6: windowClause ({@link SqlNodeList})</li>
 * <li>7: orderClause ({@link SqlNode})</li>
 * </ul>
 */
public class TrainDBSqlSelectOperator extends SqlOperator {
  public static final TrainDBSqlSelectOperator INSTANCE = new TrainDBSqlSelectOperator();

  //~ Constructors -----------------------------------------------------------

  private TrainDBSqlSelectOperator() {
    super("SELECT", SqlKind.SELECT, 2, true, ReturnTypes.SCOPE, null, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    assert functionQualifier == null;
    return new TrainDBSqlSelect(pos,
        (SqlNodeList) operands[0],
        (SqlNodeList) operands[1],
        requireNonNull((SqlNodeList) operands[2], "selectList"),
        operands[3],
        operands[4],
        (SqlNodeList) operands[5],
        operands[6],
        (SqlNodeList) operands[7],
        (SqlNodeList) operands[8],
        operands[9],
        operands[10],
        (SqlNodeList) operands[11]);
  }

  @Override public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (!onlyExpressions) {
      // None of the arguments to the SELECT operator are expressions.
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  @SuppressWarnings("deprecation")
  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    TrainDBSqlSelect select = (TrainDBSqlSelect) call;
    final SqlWriter.Frame selectFrame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("SELECT");

    if (select.hasHints()) {
      writer.sep("/*+");
      castNonNull(select.getHints()).unparse(writer, 0, 0);
      writer.print("*/");
      writer.newlineAndIndent();
    }

    for (int i = 0; i < select.getKeywordList().size(); i++) {
      final SqlLiteral keyword = (SqlLiteral) select.getKeywordList().get(i);
      if (keyword.getValue() == TrainDBSqlSelectKeyword.APPROXIMATE) { // FIXME temporary
        continue;
      }
      keyword.unparse(writer, 0, 0);
    }
    writer.topN(select.getFetch(), select.getOffset());
    final SqlNodeList selectClause = select.getSelectList();
    writer.list(SqlWriter.FrameTypeEnum.SELECT_LIST, SqlWriter.COMMA,
        selectClause);

    if (select.getFrom() != null) {
      // Calcite SQL requires FROM but MySQL does not.
      writer.sep("FROM");

      // for FROM clause, use precedence just below join operator to make
      // sure that an un-joined nested select will be properly
      // parenthesized
      final SqlWriter.Frame fromFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
      select.getFrom().unparse(
          writer,
          SqlJoin.OPERATOR.getLeftPrec() - 1,
          SqlJoin.OPERATOR.getRightPrec() - 1);
      writer.endList(fromFrame);
    }

    SqlNode where = select.getWhere();
    if (where != null) {
      writer.sep("WHERE");

      if (!writer.isAlwaysUseParentheses()) {
        SqlNode node = where;

        // decide whether to split on ORs or ANDs
        SqlBinaryOperator whereSep = SqlStdOperatorTable.AND;
        if ((node instanceof SqlCall)
            && node.getKind() == SqlKind.OR) {
          whereSep = SqlStdOperatorTable.OR;
        }

        // unroll whereClause
        final List<SqlNode> list = new ArrayList<>(0);
        while (node.getKind() == whereSep.kind) {
          assert node instanceof SqlCall;
          final SqlCall call1 = (SqlCall) node;
          list.add(0, call1.operand(1));
          node = call1.operand(0);
        }
        list.add(0, node);

        // unparse in a WHERE_LIST frame
        writer.list(SqlWriter.FrameTypeEnum.WHERE_LIST, whereSep,
            new SqlNodeList(list, where.getParserPosition()));
      } else {
        where.unparse(writer, 0, 0);
      }
    }
    if (select.getGroup() != null) {
      writer.sep("GROUP BY");
      final SqlNodeList groupBy =
          select.getGroup().size() == 0 ? SqlNodeList.SINGLETON_EMPTY
              : select.getGroup();
      writer.list(SqlWriter.FrameTypeEnum.GROUP_BY_LIST, SqlWriter.COMMA,
          groupBy);
    }
    if (select.getHaving() != null) {
      writer.sep("HAVING");
      select.getHaving().unparse(writer, 0, 0);
    }
    if (select.getWindowList().size() > 0) {
      writer.sep("WINDOW");
      writer.list(SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST, SqlWriter.COMMA,
          select.getWindowList());
    }
    if (select.getOrderList() != null && select.getOrderList().size() > 0) {
      writer.sep("ORDER BY");
      writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
          select.getOrderList());
    }
    writer.fetchOffset(select.getFetch(), select.getOffset());
    writer.endList(selectFrame);
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal == SqlSelect.WHERE_OPERAND;
  }
}
