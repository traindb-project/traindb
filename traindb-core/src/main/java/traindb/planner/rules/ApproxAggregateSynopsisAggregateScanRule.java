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
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
import traindb.catalog.pm.MSynopsis;
import traindb.planner.TrainDBPlanner;

@Value.Enclosing
public class ApproxAggregateSynopsisAggregateScanRule
    extends RelRule<ApproxAggregateSynopsisAggregateScanRule.Config>
    implements SubstitutionRule {

  protected ApproxAggregateSynopsisAggregateScanRule(Config config) {
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
    final TableScan scan = call.rel(1);

    List<Integer> requiredColumnIndex = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      requiredColumnIndex.addAll(aggCall.getArgList());
    }

    List<String> inputColumns = scan.getRowType().getFieldNames();
    List<String> requiredColumnNames =
        ApproxAggregateUtil.getSublistByIndex(inputColumns, requiredColumnIndex);
    List<String> qualifiedTableName = scan.getTable().getQualifiedName();
    Collection<MSynopsis> candidateSynopses =
        planner.getAvailableSynopses(qualifiedTableName, requiredColumnNames);
    if (candidateSynopses == null || candidateSynopses.isEmpty()) {
      return;
    }

    MSynopsis bestSynopsis = planner.getBestSynopsis(candidateSynopses, scan, aggregate.getHints());

    List<Integer> targets = new ArrayList<>();
    for (int i = 0; i < inputColumns.size(); i++) {
      targets.add(bestSynopsis.getModel().getColumnNames().indexOf(inputColumns.get(i)));
    }
    final Mappings.TargetMapping mapping = Mappings.source(targets, targets.size());

    RelOptTableImpl synopsisTable =
        (RelOptTableImpl) planner.getSynopsisTable(bestSynopsis, scan.getTable());
    TableScan newScan = new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
        (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());

    List<RexNode> aggProjects =
        ApproxAggregateUtil.makeAggregateProjects(aggregate, scan.getTable(), synopsisTable);
    List<RexNode> newGroupSet = ApproxAggregateUtil.makeAggregateGroupSet(aggregate, mapping);

    final ImmutableList.Builder<AggregateCall> newAggCalls = ImmutableList.builder();
    aggregate.getAggCallList()
        .forEach(aggregateCall -> newAggCalls.add(aggregateCall.transform(mapping)));

    RelNode node = relBuilder.push(newScan)
        .aggregate(relBuilder.groupKey(newGroupSet), newAggCalls.build())
        .project(aggProjects, aggregate.getRowType().getFieldNames())
        .build();

    call.transformTo(node);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableApproxAggregateSynopsisAggregateScanRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .predicate(ApproxAggregateUtil::isApproximateAggregate)
                .oneInput(b1 ->
                    b1.operand(TableScan.class).noInputs()));

    @Override
    default ApproxAggregateSynopsisAggregateScanRule toRule() {
      return new ApproxAggregateSynopsisAggregateScanRule(this);
    }
  }
}