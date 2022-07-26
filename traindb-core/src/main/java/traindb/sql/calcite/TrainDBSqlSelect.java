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

package traindb.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.Span;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TrainDBSqlSelect extends SqlSelect {

  SqlNodeList traindbKeywordList;

  public TrainDBSqlSelect(SqlParserPos pos,
                          @Nullable SqlNodeList keywordList,
                          @Nullable SqlNodeList traindbKeywordList,
                          SqlNodeList selectList,
                          @Nullable SqlNode from,
                          @Nullable SqlNode where,
                          @Nullable SqlNodeList groupBy,
                          @Nullable SqlNode having,
                          @Nullable SqlNodeList windowDecls,
                          @Nullable SqlNodeList orderBy,
                          @Nullable SqlNode offset,
                          @Nullable SqlNode fetch,
                          @Nullable SqlNodeList hints) {
    super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset,
          fetch, hints);
    this.traindbKeywordList = traindbKeywordList;
    if (isKeywordPresent(TrainDBSqlSelectKeyword.APPROXIMATE)) {
      List<SqlNode> hintList;
      if (hints == null) {
        hintList = new ArrayList<SqlNode>();
      } else {
        hintList = getHints().getList();
      }
      hintList.add(new SqlHint(pos, new SqlIdentifier("APPROXIMATE_AGGR", pos),
          SqlNodeList.EMPTY, SqlHint.HintOptionFormat.EMPTY));
      setHints(new SqlNodeList(hintList, pos));

      SqlParserPos fromPos = Span.of(from).end(from);
      hintList.add(new SqlHint(fromPos, new SqlIdentifier("APPROXIMATE_AGGR_TABLE", fromPos),
          SqlNodeList.EMPTY, SqlHint.HintOptionFormat.EMPTY));
      setHints(new SqlNodeList(hintList, fromPos));
    }
  }

  @Override
  public SqlOperator getOperator() {
    return TrainDBSqlSelectOperator.INSTANCE;
  }

  public SqlNodeList getKeywordList() {
    return traindbKeywordList;
  }

  public boolean isKeywordPresent(TrainDBSqlSelectKeyword targetKeyWord) {
    return getModifierNode(targetKeyWord) != null;
  }

  public final @Nullable SqlNode getModifierNode(TrainDBSqlSelectKeyword modifier) {
    for (SqlNode keyword : traindbKeywordList) {
      TrainDBSqlSelectKeyword keyword2 =
          ((SqlLiteral) keyword).symbolValue(TrainDBSqlSelectKeyword.class);
      if (keyword2 == modifier) {
        return keyword;
      }
    }
    return null;
  }

}