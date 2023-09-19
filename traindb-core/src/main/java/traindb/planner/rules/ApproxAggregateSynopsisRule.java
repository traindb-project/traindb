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
import java.util.HashSet;
import java.util.List;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
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

  private List<Integer> getRequiredColumnIndex(RelNode node) {
    List<Integer> requiredColumnIndex = new ArrayList<>();

    if (node instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) node;
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        requiredColumnIndex.addAll(aggCall.getArgList());
      }
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        requiredColumnIndex.addAll(aggregate.getGroupSets().get(i).asList());
      }
    } else if (node instanceof Project) {
      Project project = (Project) node;
      requiredColumnIndex.addAll(getRexInputRefIndex(project.getProjects()));
    } else if (node instanceof Filter) {
      Filter filter = (Filter) node;
      List<RexNode> operands = ((RexCall) filter.getCondition()).getOperands();
      requiredColumnIndex.addAll(getRexInputRefIndex(operands));
    } else if (node instanceof Join) {
      Join join = (Join) node;
      List<RexNode> operands = ((RexCall) join.getCondition()).getOperands();
      requiredColumnIndex.addAll(getRexInputRefIndex(operands));
    }

    return requiredColumnIndex;
  }

  private Mappings.TargetMapping createMapping(List<String> fromColumns, List<String> toColumns) {
    List<Integer> targets = new ArrayList<>();
    for (int i = 0; i < fromColumns.size(); i++) {
      targets.add(toColumns.indexOf(fromColumns.get(i)));
    }
    return Mappings.source(targets, targets.size());
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
    for (TableScan scan : tableScans) {
      if (!isApplicable(aggregate, scan)) {
        continue;
      }

      Set<Integer> requiredColumnIndex = new HashSet<>();
      RelNode node, parent;
      for (node = scan, parent = ApproxAggregateUtil.getParent(aggregate, scan);
           node != aggregate;
           node = parent, parent = ApproxAggregateUtil.getParent(aggregate, node)) {
        requiredColumnIndex.addAll(getRequiredColumnIndex(parent));
        if (parent instanceof Project) {
          break;
        }
      }
      List<String> inputColumns = scan.getRowType().getFieldNames();
      List<String> requiredColumnNames =
          ApproxAggregateUtil.getSublistByIndex(inputColumns, new ArrayList(requiredColumnIndex));

      List<String> qualifiedTableName = scan.getTable().getQualifiedName();
      Collection<MSynopsis> candidateSynopses =
          planner.getAvailableSynopses(qualifiedTableName, requiredColumnNames);
      if (candidateSynopses == null || candidateSynopses.isEmpty()) {
        continue;
      }

      MSynopsis bestSynopsis =
          planner.getBestSynopsis(candidateSynopses, scan, aggregate.getHints());
      RelOptTableImpl synopsisTable =
          (RelOptTableImpl) planner.getSynopsisTable(bestSynopsis, scan.getTable());
      TableScan newScan = new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
          (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());
      relBuilder.push(newScan);

      final List<String> synopsisColumns = bestSynopsis.getModel().getColumnNames();
      final Mappings.TargetMapping mapping = createMapping(inputColumns, synopsisColumns);

      boolean projected = false;
      RelNode child;
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

      List<RexNode> aggProjects =
          ApproxAggregateUtil.makeAggregateProjects(aggregate, scan.getTable(), synopsisTable);
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