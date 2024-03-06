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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
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
    List<Integer> requiredColumnIndex = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      requiredColumnIndex.addAll(aggCall.getArgList());
    }
    int aggColumnSize = requiredColumnIndex.size();

    List<TableScan> tableScans = ApproxAggregateUtil.findAllTableScans(aggregate);
    for (TableScan scan : tableScans) {
      RelNode parent = ApproxAggregateUtil.getParent(aggregate, scan);
      if (parent == null || !(parent instanceof Project)) {
        continue;
      }
      if (ApproxAggregateUtil.getParent(aggregate, parent) != aggregate) {
        continue;
      }

      requiredColumnIndex.subList(aggColumnSize, requiredColumnIndex.size()).clear();
      Project project = (Project) parent;
      List<String> inputColumns = project.getRowType().getFieldNames();
      List<String> requiredColumnNames =
          ApproxAggregateUtil.getSublistByIndex(inputColumns, requiredColumnIndex);
      Collection<MSynopsis> candidateSynopses =
          planner.getAvailableSynopses(scan.getTable().getQualifiedName(), requiredColumnNames);
      if (candidateSynopses == null || candidateSynopses.isEmpty()) {
        return;
      }

      MSynopsis bestSynopsis = planner.getBestSynopsis(
          candidateSynopses, scan, aggregate.getHints(), requiredColumnNames);

      final List<String> synopsisColumns = bestSynopsis.getColumnNames();

      List<RexNode> oldProjects = project.getProjects();
      List<RexNode> newProjects = new ArrayList<>();
      for (int i = 0; i < oldProjects.size(); i++) {
        RexInputRef inputRef = (RexInputRef) oldProjects.get(i);
        int newIndex = synopsisColumns.indexOf(inputColumns.get(i));
        newProjects.add(new RexInputRef(newIndex, inputRef.getType()));
      }

      RelOptTableImpl synopsisTable =
          (RelOptTableImpl) planner.getSynopsisTable(bestSynopsis, scan.getTable());
      TableScan newScan = new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
          (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());

      List<RexNode> aggProjects = ApproxAggregateUtil.makeAggregateProjects(
          aggregate, scan.getTable(), synopsisTable.getRowCount());

      RelNode node = relBuilder.push(newScan)
          .project(newProjects, project.getRowType().getFieldNames())
          .aggregate(relBuilder.groupKey(aggregate.getGroupSet()), aggregate.getAggCallList())
          .project(aggProjects, aggregate.getRowType().getFieldNames())
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
                .anyInputs());

    @Override
    default ApproxAggregateSynopsisProjectScanRule toRule() {
      return new ApproxAggregateSynopsisProjectScanRule(this);
    }
  }
}
