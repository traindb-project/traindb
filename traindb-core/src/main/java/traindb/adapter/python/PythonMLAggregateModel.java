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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import traindb.engine.AbstractTrainDBModelRunner;

public class PythonMLAggregateModel extends AbstractTable implements ScannableTable {

  public final AbstractTrainDBModelRunner runner;
  public final List<AggregateCall> aggCalls;
  public final ImmutableBitSet groupSet;
  public final RelDataType inputRowType;
  public final Map<RexNode, RelDataType> filterConditionMap;
  public final RelDataType rowType;

  public PythonMLAggregateModel(
      AbstractTrainDBModelRunner runner, List<AggregateCall> aggCalls, ImmutableBitSet groupSet,
      RelDataType inputRowType, Map<RexNode, RelDataType> filterConditionMap, RelDataType rowType) {
    this.runner = runner;
    this.aggCalls = aggCalls;
    this.groupSet = groupSet;
    this.inputRowType = inputRowType;
    this.filterConditionMap = filterConditionMap;
    this.rowType = rowType;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    String csvResultPath = doInfer();
    Map<String, SqlTypeName> fieldsMap = new LinkedHashMap<>();
    for (RelDataTypeField f : rowType.getFieldList()) {
      fieldsMap.put(f.getName(), f.getType().getSqlTypeName());
    }

    return new PythonMLAggregateEnumerable(csvResultPath, fieldsMap);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.copyType(rowType);
  }

  public String doInfer() {
    PythonRexToSqlNodeConverterImpl rexToSqlNodeConverter =
        new PythonRexToSqlNodeConverterImpl(new RexSqlStandardConvertletTable());

    List<String> aggregateExpressions = new ArrayList<>();
    List<String> inputFields = inputRowType.getFieldNames();
    List<String> aggCallFieldNames = new ArrayList<>();
    for (AggregateCall aggCall : aggCalls) {
      aggCallFieldNames.clear();
      for (int i : aggCall.getArgList()) {
        aggCallFieldNames.add(inputFields.get(i));
      }
      String aggrFuncName = aggCall.getAggregation().getName();
      if ("COUNT".equalsIgnoreCase(aggrFuncName) && aggCallFieldNames.isEmpty()) {
        aggCallFieldNames.add("*");
      }
      aggregateExpressions.add(buildString(aggCallFieldNames, aggrFuncName + "(", ",", ")"));
    }
    if (aggregateExpressions.size() > 1) {
      throw new UnsupportedOperationException(
          "Two or more aggregate expressions are not supported yet.");
    }

    List<String> groupColumns = getGroupColumnNames(groupSet, inputRowType);
    if (groupColumns.size() > 1) {
      throw new UnsupportedOperationException(
          "Two or more group columns are not supported yet.");
    }

    List<String> filterConditions = new ArrayList<>();
    for (Map.Entry<RexNode, RelDataType> entry : filterConditionMap.entrySet()) {
      rexToSqlNodeConverter.setInputcolumns(entry.getValue().getFieldNames());
      SqlNode sqlNode = rexToSqlNodeConverter.convertCall((RexCall) entry.getKey());
      filterConditions.add(sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).toString().replace("`", ""));
    }
    if (filterConditions.size() > 1) {
      throw new UnsupportedOperationException(
          "Two or more filter conditions are not supported yet.");
    }

    String aggregate = aggregateExpressions.get(0);
    String groupBy = groupColumns.isEmpty() ? "" : groupColumns.get(0);
    String whereCondition = filterConditions.isEmpty() ? "" : filterConditions.get(0);

    String resultCsvPath;
    try {
      resultCsvPath = runner.infer(aggregate, groupBy, whereCondition);
    } catch (Exception e) {
      throw new RuntimeException("failed to infer expression '" + aggregate + "'");
    }
    return resultCsvPath;
  }

  private List<String> getGroupColumnNames(ImmutableBitSet groupSet, RelDataType inputRowType) {
    List<String> groupColumnNames = new ArrayList<>();
    final List<Integer> groupList = groupSet.asList();
    final List<RelDataTypeField> fieldList = inputRowType.getFieldList();
    for (int groupKey : groupList) {
      final RelDataTypeField field = fieldList.get(groupKey);
      groupColumnNames.add(field.getName());
    }
    return groupColumnNames;
  }

  private String buildString(Collection<String> items, String start, String sep, String end) {
    StringBuilder sb = new StringBuilder();
    sb.append(start);
    for (String item : items) {
      sb.append(item);
      sb.append(sep);
    }
    if (items.size() > 0) {
      sb.setLength(sb.length() - 1);
    }
    sb.append(end);
    return sb.toString();
  }
}
