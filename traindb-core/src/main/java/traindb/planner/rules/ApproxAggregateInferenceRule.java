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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;
import traindb.adapter.python.PythonMLAggregate;
import traindb.adapter.python.PythonMLAggregateModel;
import traindb.adapter.python.PythonMLAggregateModelScan;
import traindb.adapter.python.PythonRel;
import traindb.catalog.pm.MModel;
import traindb.engine.AbstractTrainDBModelRunner;
import traindb.engine.TrainDBFileModelRunner;
import traindb.planner.TrainDBPlanner;

@Value.Enclosing
public class ApproxAggregateInferenceRule
    extends RelRule<ApproxAggregateInferenceRule.Config>
    implements SubstitutionRule {

  protected ApproxAggregateInferenceRule(Config config) {
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
    List<Integer> aggColumnIndex = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      aggColumnIndex.addAll(aggCall.getArgList());
    }

    List<TableScan> tableScans = ApproxAggregateUtil.findAllTableScans(aggregate);
    if (tableScans.size() != 1) {
      return;
    }
    TableScan scan = tableScans.get(0);
    RelDataType inputRowType = scan.getRowType();
    Map<RexNode, RelDataType> filterConditionMap = new HashMap<>();
    List<String> requiredColumnNames = new ArrayList<>();

    RelNode parent = ApproxAggregateUtil.getParent(aggregate, scan);
    while (parent != aggregate) {
      if (parent instanceof Project) {
        inputRowType = parent.getRowType();
      } else if (parent instanceof Filter) {
        Filter filter = (Filter) parent;
        filterConditionMap.put(filter.getCondition(), inputRowType);

        List<RexNode> rexNodes = ((RexCall) filter.getCondition()).getOperands();
        for (RexNode rexNode : rexNodes) {
          if (rexNode instanceof RexCall) {
            rexNode = ((RexCall) rexNode).getOperands().get(0);
          }
          if (rexNode instanceof RexInputRef) {
            requiredColumnNames.add(
                inputRowType.getFieldNames().get(((RexInputRef) rexNode).getIndex()));
          }
        }
      }
      parent = ApproxAggregateUtil.getParent(aggregate, parent);
    }

    requiredColumnNames.addAll(
        ApproxAggregateUtil.getSublistByIndex(inputRowType.getFieldNames(), aggColumnIndex));
    Collection<MModel> candidateModels =
        planner.getAvailableInferenceModels(scan.getTable().getQualifiedName(),
            requiredColumnNames);
    if (candidateModels == null || candidateModels.isEmpty()) {
      return;
    }

    // TODO choose the best inference model
    final MModel bestInferenceModel = candidateModels.iterator().next();

    AbstractTrainDBModelRunner runner =
        new TrainDBFileModelRunner(null, planner.getCatalogContext(),
            bestInferenceModel.getModeltype().getName(), bestInferenceModel.getName());

    PythonMLAggregateModel modelTable = new PythonMLAggregateModel(runner,
        aggregate.getAggCallList(), aggregate.getGroupSet(), aggregate.getInput().getRowType(),
        filterConditionMap, aggregate.getRowType());
    PythonMLAggregate mlAgg;
    try {
      RelOptCluster cluster = aggregate.getCluster();
      mlAgg = new PythonMLAggregate(
          cluster, cluster.traitSetOf(PythonRel.CONVENTION),
          new PythonMLAggregateModelScan(
              cluster, cluster.traitSetOf(PythonRel.CONVENTION), scan.getTable(), modelTable),
          aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList(),
          bestInferenceModel);
      mlAgg.register(planner);
    } catch (InvalidRelException e) {
      return;
    }

    RelNode node = relBuilder.push(mlAgg).build();
    call.transformTo(node);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableApproxAggregateInferenceRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .predicate(ApproxAggregateUtil::isApproximateAggregate)
                .anyInputs());

    @Override
    default ApproxAggregateInferenceRule toRule() {
      return new ApproxAggregateInferenceRule(this);
    }
  }
}
