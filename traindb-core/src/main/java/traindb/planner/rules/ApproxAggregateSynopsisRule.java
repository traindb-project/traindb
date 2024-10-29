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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import traindb.catalog.pm.MSynopsis;
import traindb.planner.TrainDBPlanner;

@Value.Enclosing
public class ApproxAggregateSynopsisRule
    extends RelRule<ApproxAggregateSynopsisRule.Config>
    implements SubstitutionRule {

  protected ApproxAggregateSynopsisRule(Config config) {
    super(config);
  }

  @Override
  public boolean autoPruneOld() {
    return true;
  }

  private boolean isApplicable(Aggregate aggregate, RelNode node) {
    RelNode parent = ApproxAggregateUtil.getParent(aggregate, node);
    if (parent == null) {
      return false;
    }
    if (parent == aggregate) {
      return true;
    }
    if (parent instanceof Filter
        || parent instanceof Project
        || parent instanceof Join) {
      return isApplicable(aggregate, parent);
    }
    return false;
  }

  private List<Integer> getRexInputRefIndex(List<RexNode> rexNodes) {
    List<Integer> rexInputRefIndex = new ArrayList<>();
    for (RexNode rexNode : rexNodes) {
      if (rexNode instanceof RexCall) {
        List<RexNode> operands = ((RexCall) rexNode).getOperands();
        for (RexNode operand : operands) {
          if (operand instanceof RexInputRef) {
            rexInputRefIndex.add(((RexInputRef) operand).getIndex());
          }
        }
      }
      if (rexNode instanceof RexInputRef) {
        rexInputRefIndex.add(((RexInputRef) rexNode).getIndex());
      }
    }
    return rexInputRefIndex;
  }

  private void addRequiredColumnIndex(List<Integer> requiredColumnIndex,
                                      List<Integer> inputRefIndex,
                                      int start, int end) {
    for (int i = 0; i < inputRefIndex.size(); i++) {
      int idx = inputRefIndex.get(i);
      if (idx >= start && idx < end) {
        requiredColumnIndex.add(idx - start);
      }
    }
  }

  private List<Integer> getRequiredColumnIndex(RelNode node, int start, int end) {
    List<Integer> requiredColumnIndex = new ArrayList<>();

    if (node instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) node;
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        addRequiredColumnIndex(requiredColumnIndex, aggCall.getArgList(), start, end);
      }
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        addRequiredColumnIndex(
            requiredColumnIndex, aggregate.getGroupSets().get(i).asList(), start, end);
      }
    } else if (node instanceof Project) {
      Project project = (Project) node;
      addRequiredColumnIndex(
          requiredColumnIndex, getRexInputRefIndex(project.getProjects()), start, end);
    } else if (node instanceof Filter) {
      Filter filter = (Filter) node;
      List<RexNode> operands = ((RexCall) filter.getCondition()).getOperands();
      addRequiredColumnIndex(requiredColumnIndex, getRexInputRefIndex(operands), start, end);
    } else if (node instanceof Join) {
      Join join = (Join) node;
      if (join.getCondition() != null && !(join.getCondition() instanceof RexLiteral)) {
        List<RexNode> operands = ((RexCall) join.getCondition()).getOperands();
        addRequiredColumnIndex(requiredColumnIndex, getRexInputRefIndex(operands), start, end);
      }
    }

    return requiredColumnIndex;
  }

  private List<String> getConditionString(RelNode node, List<TableScan> scans) {
    List<String> equivConditions = new ArrayList<>();
    if (node instanceof Join) {
      Join join = (Join) node;
      RexNode joinCondition = join.getCondition();
      if (joinCondition instanceof RexCall) {
        List<RexNode> operands = ((RexCall) joinCondition).getOperands();
        List<Integer> inputRefIndex = getRexInputRefIndex(operands);
        SqlOperator operator = ((RexCall) joinCondition).getOperator();
        int li;
        int ri;
        if (inputRefIndex.get(0) < inputRefIndex.get(1)) {
          li = inputRefIndex.get(0);
          ri = inputRefIndex.get(1) - join.getLeft().getRowType().getFieldCount();
        } else {
          li = inputRefIndex.get(1);
          ri = inputRefIndex.get(0) - join.getLeft().getRowType().getFieldCount();
        }

        String lc = scans.get(0).getTable().getQualifiedName().get(2)
            + "." + join.getLeft().getRowType().getFieldNames().get(li);
        String rc = scans.get(1).getTable().getQualifiedName().get(2)
            + "." + join.getRight().getRowType().getFieldNames().get(ri);

        StringBuilder sb = new StringBuilder();
        sb.append(lc).append(" ").append(operator).append(" ").append(rc);
        equivConditions.add(sb.toString());

        sb.setLength(0);
        sb.append(rc).append(" ").append(swapOperator(operator.toString())).append(" ").append(lc);
        equivConditions.add(sb.toString());
      } else if (joinCondition instanceof RexLiteral) {
        equivConditions.add("");
      }
    }

    return equivConditions;
  }

  private String swapOperator(String operator) {
    if (operator.equals(">")) {
      return "<";
    }
    if (operator.equals("<")) {
      return ">";
    }
    if (operator.equals(">=")) {
      return "<=";
    }
    if (operator.equals("<=")) {
      return ">=";
    }
    return operator;
  }

  private Mappings.TargetMapping createMapping(List<String> fromColumns, List<String> toColumns) {
    List<Integer> targets = new ArrayList<>();
    for (int i = 0; i < fromColumns.size(); i++) {
      targets.add(toColumns.indexOf(fromColumns.get(i)));
    }
    return Mappings.source(targets, toColumns.size());
  }

  //~ Methods ----------------------------------------------------------------
  @Override
  public void onMatch(RelOptRuleCall call) {
    if (!(call.getPlanner() instanceof TrainDBPlanner)) {
      return;
    }
    final TrainDBPlanner planner = (TrainDBPlanner) call.getPlanner();
    final RelBuilder relBuilder = call.builder();

    final Aggregate aggregate = call.rel(0);
    List<TableScan> tableScans = ApproxAggregateUtil.findAllTableScans(aggregate);
    Map<Join, List<TableScan>> joinScanMap = new HashMap<>();
    Map<Join, List<Filter>> joinFilterMap = new HashMap<>();
    Map<TableScan, List<String>> requiredScanColumnMap = new HashMap<>();
    for (TableScan scan : tableScans) {
      if (!isApplicable(aggregate, scan)) {
        continue;
      }

      // build required column information for each table scan
      Set<Integer> requiredColumnIndex = new HashSet<>();
      List<Filter> filterList = new ArrayList<>();
      int start = 0;
      int end = scan.getRowType().getFieldCount();
      boolean projected = false;
      RelNode node;
      RelNode parent;
      for (node = scan, parent = ApproxAggregateUtil.getParent(aggregate, scan);
           node != aggregate;
           node = parent, parent = ApproxAggregateUtil.getParent(aggregate, node)) {
        if (parent instanceof Join) {
          Join parentJoin = (Join) parent;
          List<TableScan> joinScans = joinScanMap.get(parentJoin);
          if (joinScans == null) {
            joinScans = new ArrayList<>();
            joinScans.add(scan);
            joinScanMap.put(parentJoin, joinScans);
          } else {
            joinScans.add(scan);
          }
          if (!filterList.isEmpty()) {
            List<Filter> joinFilters = joinFilterMap.get(parentJoin);
            if (joinFilters == null) {
              joinFilterMap.put(parentJoin, filterList);
            } else {
              joinFilters.addAll(filterList);
            }
          }
          if (projected) {
            continue;
          }

          RelNode left = parentJoin.getLeft();
          if (left instanceof RelSubset) {
            left = ((RelSubset) left).getBestOrOriginal();
          }
          RelNode right = parentJoin.getRight();
          if (right instanceof RelSubset) {
            right = ((RelSubset) right).getBestOrOriginal();
          }
          if (node == right) {
            start = left.getRowType().getFieldCount();
            end = left.getRowType().getFieldCount() + right.getRowType().getFieldCount();
          }
        } else if (parent instanceof Filter) {
          filterList.add((Filter) parent);
        }

        if (projected) {
          continue;
        }

        requiredColumnIndex.addAll(getRequiredColumnIndex(parent, start, end));
        if (parent instanceof Project) {
          projected = true;
        }
      }

      List<String> inputColumns = scan.getRowType().getFieldNames();
      List<String> requiredColumnNames =
          ApproxAggregateUtil.getSublistByIndex(inputColumns, new ArrayList(requiredColumnIndex));
      requiredScanColumnMap.put(scan, requiredColumnNames);
    }

    // try join synopses first
    for (Map.Entry<Join, List<TableScan>> entry : joinScanMap.entrySet()) {
      Join join = entry.getKey();
      List<TableScan> joinScans = entry.getValue();
      if (joinScans.size() < 2) {
        continue;
      }
      List<String> condStr = getConditionString(join, joinScans);
      Collection<MSynopsis> candidateJoinSynopses =
          planner.getAvailableJoinSynopses(joinScans, requiredScanColumnMap, condStr);
      if (candidateJoinSynopses == null || candidateJoinSynopses.isEmpty()) {
        continue;
      }
      List<String> requiredJoinColumnNames = new ArrayList<>();
      for (TableScan joinScan : joinScans) {
        requiredJoinColumnNames.addAll(requiredScanColumnMap.get(joinScan));
      }

      TableScan baseScan = joinScans.get(0);
      MSynopsis bestSynopsis = planner.getBestSynopsis(
          candidateJoinSynopses, baseScan, aggregate.getHints(), requiredJoinColumnNames);

      RelOptTableImpl synopsisTable =
          (RelOptTableImpl) planner.getSynopsisTable(bestSynopsis, baseScan.getTable());
      if (synopsisTable == null) {
        return;
      }
      TableScan newScan = planner.createSynopsisTableScan(bestSynopsis, synopsisTable, baseScan);
      relBuilder.push(newScan);

      List<String> synopsisColumns = bestSynopsis.getColumnNames();
      List<String> inputColumns = new ArrayList<>();
      inputColumns.addAll(join.getLeft().getRowType().getFieldNames());
      inputColumns.addAll(join.getRight().getRowType().getFieldNames());
      if (inputColumns.size() > synopsisColumns.size()) {
        continue;
      }

      List<Filter> joinFilters = joinFilterMap.get(join);
      if (joinFilters != null) {
        for (Filter f : joinFilters) {
          final Mappings.TargetMapping filterInputMapping =
              createMapping(f.getInput().getRowType().getFieldNames(), synopsisColumns);
          final RexNode newCondition = RexUtil.apply(filterInputMapping, f.getCondition());
          relBuilder.filter(newCondition);
        }
      }

      final Mappings.TargetMapping mapping = createMapping(inputColumns, synopsisColumns);
      relBuilder.project(relBuilder.fields(mapping), join.getRowType().getFieldNames(), true);

      RelNode child;
      RelNode node;
      for (child = join, node = ApproxAggregateUtil.getParent(aggregate, join);
           node != aggregate; child = node, node = ApproxAggregateUtil.getParent(aggregate, node)) {
        if (node instanceof Filter) {
          Filter filter = (Filter) node;
          relBuilder.filter(filter.getCondition());
        } else if (node instanceof Join) {
          Join oj = (Join) node;
          RexNode newCondition;
          newCondition = oj.getCondition();
          RelNode left = oj.getLeft();
          RelNode right = oj.getRight();
          if (left instanceof RelSubset
              && ((RelSubset) left).getBestOrOriginal() == child) {
            final Join newJoin =
                oj.copy(oj.getTraitSet(), newCondition, relBuilder.peek(), oj.getRight(),
                    oj.getJoinType(), oj.isSemiJoinDone());
            relBuilder.clear();
            relBuilder.push(newJoin);
          } else if (right instanceof RelSubset
              && ((RelSubset) right).getBestOrOriginal() == child) {
            final Join newJoin =
                oj.copy(oj.getTraitSet(), newCondition, oj.getLeft(), relBuilder.peek(),
                    oj.getJoinType(), oj.isSemiJoinDone());
            relBuilder.clear();
            relBuilder.push(newJoin);
          } else {
            return;
          }
        } else if (node instanceof Project) {
          Project project = (Project) node;
          relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
        } else {
          break; /* cannot apply this rule */
        }
      }

      if (node != aggregate) {
        continue;
      }
      relBuilder.aggregate(relBuilder.groupKey(aggregate.getGroupSet()),
          aggregate.getAggCallList());

      double scaleFactor = 1.0 / bestSynopsis.getRatio();
      List<RexNode> aggProjects = ApproxAggregateUtil.makeAggregateProjects(aggregate, scaleFactor);
      relBuilder.project(aggProjects, aggregate.getRowType().getFieldNames());

      call.transformTo(relBuilder.build());
      return;
    }

    // apply synopses on single tables
    for (Map.Entry<TableScan, List<String>> entry : requiredScanColumnMap.entrySet()) {
      TableScan scan = entry.getKey();
      List<String> requiredColumnNames = entry.getValue();
      List<String> inputColumns = scan.getRowType().getFieldNames();

      List<String> qualifiedTableName = scan.getTable().getQualifiedName();
      Collection<MSynopsis> candidateSynopses =
          planner.getAvailableSynopses(qualifiedTableName, requiredColumnNames);
      if (candidateSynopses == null || candidateSynopses.isEmpty()) {
        continue;
      }

      MSynopsis bestSynopsis = planner.getBestSynopsis(
          candidateSynopses, scan, aggregate.getHints(), requiredColumnNames);
      if (bestSynopsis == null) {
        continue;
      }
      RelOptTableImpl synopsisTable =
          (RelOptTableImpl) planner.getSynopsisTable(bestSynopsis, scan.getTable());
      if (synopsisTable == null) {
        return;
      }
      TableScan newScan = planner.createSynopsisTableScan(bestSynopsis, synopsisTable, scan);
      relBuilder.push(newScan);

      final List<String> synopsisColumns = bestSynopsis.getColumnNames();
      final Mappings.TargetMapping mapping = createMapping(inputColumns, synopsisColumns);

      boolean projected = false;
      RelNode child;
      RelNode node;
      for (child = scan, node = ApproxAggregateUtil.getParent(aggregate, scan);
           node != aggregate; child = node, node = ApproxAggregateUtil.getParent(aggregate, node)) {
        if (node instanceof Filter) {
          Filter filter = (Filter) node;
          if (projected) {
            relBuilder.filter(filter.getCondition());
          } else {
            final RexNode newCondition = RexUtil.apply(mapping, filter.getCondition());
            relBuilder.filter(newCondition);
          }
        } else if (node instanceof Join) {
          Join join = (Join) node;
          RexNode newCondition;
          if (projected) {
            newCondition = join.getCondition();
          } else {
            newCondition = RexUtil.apply(mapping, join.getCondition());
          }
          RelNode left = join.getLeft();
          RelNode right = join.getRight();
          if (left instanceof RelSubset
              && ((RelSubset) left).getBestOrOriginal() == child) {
            final Join newJoin =
                join.copy(join.getTraitSet(), newCondition, relBuilder.peek(), join.getRight(),
                    join.getJoinType(), join.isSemiJoinDone());
            relBuilder.clear();
            relBuilder.push(newJoin);
          } else if (right instanceof RelSubset
              && ((RelSubset) right).getBestOrOriginal() == child) {
            final Join newJoin =
                join.copy(join.getTraitSet(), newCondition, join.getLeft(), relBuilder.peek(),
                    join.getJoinType(), join.isSemiJoinDone());
            relBuilder.clear();
            relBuilder.push(newJoin);
          } else {
            return;
          }
        } else if (node instanceof Project) {
          Project project = (Project) node;
          List<RexNode> oldProjects = project.getProjects();
          List<RexNode> newProjects = new ArrayList<>();
          if (projected) {
            newProjects = oldProjects;
          } else {
            for (int i = 0; i < oldProjects.size(); i++) {
              newProjects.add(RexUtil.apply(mapping, oldProjects.get(i)));
            }
          }
          relBuilder.project(newProjects, project.getRowType().getFieldNames());
          projected = true;
        } else {
          return; /* cannot apply this rule */
        }
      }

      if (projected) {
        relBuilder.aggregate(relBuilder.groupKey(aggregate.getGroupSet()),
            aggregate.getAggCallList());
      } else {
        final ImmutableList.Builder<AggregateCall> newAggCalls = ImmutableList.builder();
        List<RexNode> newGroupSet = ApproxAggregateUtil.makeAggregateGroupSet(aggregate, mapping);
        aggregate.getAggCallList()
            .forEach(aggregateCall -> newAggCalls.add(aggregateCall.transform(mapping)));
        relBuilder.aggregate(relBuilder.groupKey(newGroupSet), newAggCalls.build());
      }

      double scaleFactor = scan.getTable().getRowCount() / synopsisTable.getRowCount();
      List<RexNode> aggProjects = ApproxAggregateUtil.makeAggregateProjects(aggregate, scaleFactor);
      relBuilder.project(aggProjects, aggregate.getRowType().getFieldNames());

      call.transformTo(relBuilder.build());
    }
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableApproxAggregateSynopsisRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .predicate(ApproxAggregateUtil::isApproximateAggregate)
                .anyInputs());

    @Override
    default ApproxAggregateSynopsisRule toRule() {
      return new ApproxAggregateSynopsisRule(this);
    }
  }
}