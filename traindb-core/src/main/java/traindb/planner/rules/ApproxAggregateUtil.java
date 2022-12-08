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

package traindb.planner.rules;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Multimap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.adapter.python.PythonMLAggregateModelScan;

public class ApproxAggregateUtil {

  private static final List<String> approxAggregateFuncList = Arrays.asList(
      "AVG",
      "COUNT",
      "STDDEV",
      "SUM",
      "VARIANCE"
  );

  private static final List<String> scalingAggregateFuncList = Arrays.asList(
      "COUNT",
      "SUM"
  );

  public static boolean isApproximateAggregate(Aggregate aggregate) {
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!approxAggregateFuncList.contains(aggCall.getAggregation().getName())) {
        return false;
      }
    }
    List<RelHint> hints = aggregate.getHints();
    for (RelHint hint : hints) {
      if (hint.hintName.equals("approximate")) {
        return true;
      }
    }
    return false;
  }

  public static boolean isScalingAggregateFunction(String aggFuncName) {
    return scalingAggregateFuncList.contains(aggFuncName);
  }

  /**
   * Returns a list of all table scans used by this expression or its children.
   */
  public static List<TableScan> findAllTableScans(RelNode rel) {
    final Multimap<Class<? extends RelNode>, RelNode> nodes =
        rel.getCluster().getMetadataQuery().getNodeTypes(rel);
    final List<TableScan> usedTableScans = new ArrayList<>();
    if (nodes == null) {
      return usedTableScans;
    }
    for (Map.Entry<Class<? extends RelNode>, Collection<RelNode>> e : nodes.asMap().entrySet()) {
      if (TableScan.class.isAssignableFrom(e.getKey())) {
        for (RelNode node : e.getValue()) {
          if (node instanceof PythonMLAggregateModelScan) {
            continue;
          }
          usedTableScans.add((TableScan) node);
        }
      }
    }
    return usedTableScans;
  }

  /**
   * Returns the parent of the target node under input ancestor node.
   */
  public static RelNode getParent(RelNode ancestor, final RelNode target) {
    if (ancestor == target) {
      return null;
    }
    final List<RelNode> parentNodes = new ArrayList<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node == target) {
          parentNodes.add(parent);
        }
        for (RelNode input : node.getInputs()) {
          if (input instanceof RelSubset) {
            visit(((RelSubset) input).getBestOrOriginal(), ordinal, node);
          } else {
            visit(input, ordinal, node);
          }
        }
      }
    }.go(ancestor);
    if (parentNodes.size() == 1) {
      return parentNodes.get(0);
    }
    return null;
  }

  public static List<String> getQualifiedName(String catalog, String schema, String table) {
    return new ArrayList(Arrays.asList(catalog, schema, table));
  }

  public static <T> List<T> getSublistByIndex(List<T> list, List<Integer> index) {
    return index.stream().map(list::get).collect(toList());
  }

  public static List<RexNode> makeAggregateGroupSet(Aggregate aggregate,
                                                    Mappings.TargetMapping mapping) {
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    List<RexNode> newGroupSet = new ArrayList<>();
    for (int key : aggregate.getGroupSet()) {
      RelDataType rowTypeForKey =
          aggregate.getInput().getRowType().getFieldList().get(key).getType();
      newGroupSet.add(rexBuilder.makeInputRef(rowTypeForKey, mapping.getTarget(key)));
    }
    return newGroupSet;
  }

  public static List<RexNode> makeAggregateProjects(Aggregate aggregate,
                                                    RelOptTable baseTable,
                                                    RelOptTable synopsisTable) {
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    List<RexNode> aggProjects = new ArrayList<>();
    for (int key : aggregate.getGroupSet()) {
      RelDataType rowTypeForKey =
          aggregate.getInput().getRowType().getFieldList().get(key).getType();
      aggProjects.add(rexBuilder.makeInputRef(rowTypeForKey, aggProjects.size()));
    }

    double scaleFactor = baseTable.getRowCount() / synopsisTable.getRowCount();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      RexNode expr;
      String aggFuncName = aggCall.getAggregation().getName();
      if (ApproxAggregateUtil.isScalingAggregateFunction(aggFuncName)) {
        expr = rexBuilder.makeCast(aggCall.getType(),
            rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(scaleFactor)),
                rexBuilder.makeInputRef(aggregate, aggProjects.size())));
      } else {
        expr = rexBuilder.makeInputRef(aggregate, aggProjects.size());
      }
      aggProjects.add(expr);
    }

    return aggProjects;
  }
}

