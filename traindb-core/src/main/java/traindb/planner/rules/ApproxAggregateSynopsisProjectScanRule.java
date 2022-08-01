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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcRules;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
import traindb.catalog.pm.MSynopsis;
import traindb.planner.TrainDBPlanner;

@Value.Enclosing
public class ApproxAggregateSynopsisProjectScanRule
    extends RelRule<ApproxAggregateSynopsisProjectScanRule.Config>
    implements TransformationRule {

  protected ApproxAggregateSynopsisProjectScanRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------
  @Override public void onMatch(RelOptRuleCall call) {
    if (!(call.getPlanner() instanceof TrainDBPlanner)) {
      return;
    }
    final TrainDBPlanner planner = (TrainDBPlanner) call.getPlanner();

    final Project project = call.rel(0);
    final TableScan scan = call.rel(1);

    List<String> tqn = scan.getTable().getQualifiedName();
    String tableSchema = tqn.get(1);
    String tableName = tqn.get(2);

    Collection<MSynopsis> candidateSynopses = planner.getAvailableSynopses(tableSchema, tableName);
    if (candidateSynopses == null || candidateSynopses.isEmpty()) {
      return;
    }

    List<RexNode> projs = project.getProjects();
    List<RexNode> newProjs = new ArrayList<>();

    MSynopsis bestSynopsis = null;
    for (MSynopsis synopsis : candidateSynopses) {
      for (int i = 0; i < projs.size(); i++) {
        RexInputRef inputRef = (RexInputRef) projs.get(i);
        int newIndex = synopsis.getModelInstance().getColumnNames()
            .indexOf(project.getRowType().getFieldNames().get(i));
        if (newIndex == -1) {
          newProjs.clear();
          break;
        }
        newProjs.add(new RexInputRef(newIndex, inputRef.getType()));
      }
      if (!newProjs.isEmpty()) {
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

    double SYNOPSIS_SIZE_RATIO = 0.01; // FIXME: this information should be taken from catalog
    RelOptTableImpl synopsisTable = (RelOptTableImpl) planner.getTable(
        synopsisNames, scan.getTable().getRowCount() * SYNOPSIS_SIZE_RATIO);
    TableScan newScan = new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
        (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());
    RelSubset subset = planner.register(newScan, null);

    JdbcRules.JdbcProject newProject = new JdbcRules.JdbcProject(
        project.getCluster(), project.getTraitSet(), subset, newProjs, project.getRowType());

    call.transformTo(newProject);
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableApproxAggregateSynopsisProjectScanRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Project.class).oneInput(b1 ->
                b1.operand(TableScan.class)
                .predicate(ApproxAggregateUtil::isApproximateTableScan)
                .noInputs()));

    @Override default ApproxAggregateSynopsisProjectScanRule toRule() {
      return new ApproxAggregateSynopsisProjectScanRule(this);
    }
  }
}
