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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
import traindb.catalog.pm.MSynopsis;
import traindb.planner.TrainDBPlanner;

@Value.Enclosing
public class ApproxAggregateSynopsisProjectScanRule
    extends RelRule<ApproxAggregateSynopsisProjectScanRule.Config>
    implements SubstitutionRule {

  public static final double DEFAULT_SYNOPSIS_SIZE_RATIO = 0.01;

  protected ApproxAggregateSynopsisProjectScanRule(Config config) {
    super(config);
  }

  @Override
  public boolean autoPruneOld() {
    return true;
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
      if (!ApproxAggregateUtil.isApproximateTableScan(scan)) {
        continue;
      }

      RelNode parent = ApproxAggregateUtil.getParent(aggregate, scan);
      if (parent == null || !(parent instanceof Project)) {
        continue;
      }
      if (ApproxAggregateUtil.getParent(aggregate, parent) != aggregate) {
        continue;
      }

      List<String> tqn = scan.getTable().getQualifiedName();
      String tableSchema = tqn.get(1);
      String tableName = tqn.get(2);

      Collection<MSynopsis> candidateSynopses =
          planner.getAvailableSynopses(tableSchema, tableName);
      if (candidateSynopses == null || candidateSynopses.isEmpty()) {
        return;
      }

      Project project = (Project) parent;
      List<RexNode> oldProjects = project.getProjects();
      List<RexNode> newProjects = new ArrayList<>();

      MSynopsis bestSynopsis = null;
      for (MSynopsis synopsis : candidateSynopses) {
        for (int i = 0; i < oldProjects.size(); i++) {
          RexInputRef inputRef = (RexInputRef) oldProjects.get(i);
          int newIndex = synopsis.getModel().getColumnNames()
              .indexOf(project.getRowType().getFieldNames().get(i));
          if (newIndex == -1) {
            newProjects.clear();
            break;
          }
          newProjects.add(new RexInputRef(newIndex, inputRef.getType()));
        }
        if (!newProjects.isEmpty()) {
          // TODO choose a synopsis
          bestSynopsis = synopsis;
        }
      }
      if (bestSynopsis == null) {
        return;
      }

      List<String> synopsisNames = new ArrayList<>();
      synopsisNames.add(tqn.get(0));
      synopsisNames.add(tqn.get(1));
      synopsisNames.add(bestSynopsis.getName());

      double ratio = bestSynopsis.getRatio();
      if (ratio == 0d) {
        ratio = scan.getTable().getRowCount() * DEFAULT_SYNOPSIS_SIZE_RATIO;
      }
      RelOptTableImpl synopsisTable = (RelOptTableImpl) planner.getTable(synopsisNames, ratio);
      TableScan newScan = new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
          (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());

      List<RexNode> aggProjects = new ArrayList<>();
      final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      for (int key : aggregate.getGroupSet()) {
        final RexInputRef ref = rexBuilder.makeInputRef(aggregate, key);
        aggProjects.add(ref);
      }

      double scaleFactor = 1.0 / ratio;
      List<AggregateCall> aggCalls = aggregate.getAggCallList();
      RelDataType rowType = aggregate.getRowType();
      for (AggregateCall aggCall : aggCalls) {
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

      RelNode node = relBuilder.push(newScan)
          .project(newProjects, project.getRowType().getFieldNames())
          .aggregate(relBuilder.groupKey(aggregate.getGroupSet()), aggCalls)
          .project(aggProjects, rowType.getFieldNames())
          .build();
      call.transformTo(node);
    }
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableApproxAggregateSynopsisProjectScanRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .predicate(ApproxAggregateUtil::isApproximateAggregate)
                .predicate(ApproxAggregateUtil::hasApproxAggregateFunctionsOnly)
                .anyInputs());

    @Override
    default ApproxAggregateSynopsisProjectScanRule toRule() {
      return new ApproxAggregateSynopsisProjectScanRule(this);
    }
  }
}
