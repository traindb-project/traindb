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

import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.checkerframework.checker.nullness.qual.Nullable;

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
    List<RelHint> hints = aggregate.getHints();
    for (RelHint hint : hints) {
      if (hint.hintName.equals("APPROXIMATE_AGGR")) {
        return true;
      }
    }
    return false;
  }

  public static boolean isApproximateTableScan(TableScan scan) {
    List<RelHint> hints = scan.getHints();
    for (RelHint hint : hints) {
      if (hint.hintName.equals("APPROXIMATE_AGGR_TABLE")) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasApproxAggregateFunctionsOnly(Aggregate aggregate) {
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!approxAggregateFuncList.contains(aggCall.getAggregation().getName())) {
        return false;
      }
    }
    return true;
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
}

