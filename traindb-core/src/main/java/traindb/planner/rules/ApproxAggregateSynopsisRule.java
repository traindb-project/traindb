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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
import traindb.catalog.pm.MSynopsis;
import traindb.planner.TrainDBPlanner;

@Value.Enclosing
public class ApproxAggregateSynopsisRule
    extends RelRule<ApproxAggregateSynopsisRule.Config>
    implements TransformationRule {

  protected ApproxAggregateSynopsisRule(Config config) {
    super(config);
  }

  private static boolean isApproximateAggregate(Aggregate aggregate) {
    List<RelHint> hints = aggregate.getHints();
    for (RelHint hint : hints) {
      if (hint.hintName.equals("APPROXIMATE_AGGR")) {
        return true;
      }
    }
    return false;
  }

  private static boolean isApproximateTableScan(TableScan scan) {
    List<RelHint> hints = scan.getHints();
    for (RelHint hint : hints) {
      if (hint.hintName.equals("APPROXIMATE_AGGR_TABLE")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a list of all table scans used by this expression or its children.
   */
  private List<TableScan> findAllTableScans(RelNode rel) {
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
  private RelNode getParent(RelNode ancestor, final RelNode target) {
    if (ancestor == target) {
      return null;
    }
    final List<RelNode> parentNodes = new ArrayList<>();
    new RelVisitor() {
      @Override public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
        if (node == target) {
          parentNodes.add(parent);
        }
        for (RelNode input : node.getInputs()) {
          if (input instanceof RelSubset) {
            visit(((RelSubset) input).getBestOrOriginal(), ordinal, node);
          }
          else {
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

  //~ Methods ----------------------------------------------------------------
  @Override public void onMatch(RelOptRuleCall call) {
    if (!(call.getPlanner() instanceof TrainDBPlanner)) {
      return;
    }
    final TrainDBPlanner planner = (TrainDBPlanner) call.getPlanner();

    final Aggregate aggregate = call.rel(0);
    List<TableScan> tableScans = findAllTableScans(aggregate);
    for (TableScan scan : tableScans) {
      if (!isApproximateTableScan(scan)) {
        continue;
      }

      RelNode parent = getParent(aggregate, scan);
      if (parent == null) {
        continue;
      }
      if (!(parent instanceof Project)) {
        continue;
      }
      List<String> tqn = scan.getTable().getQualifiedName();
      String tableSchema = tqn.get(1);
      String tableName = tqn.get(2);

      Collection<MSynopsis> candidateSynopses =
          planner.getAvailableSynopses(tableSchema, tableName);
      if (candidateSynopses == null || candidateSynopses.isEmpty()) {
        continue;
      }

      Project project = (Project) parent;
      List<RexNode> projs = ((Project) parent).getProjects();
      List<RexNode> newProjs = new ArrayList<>();

      MSynopsis bestSynopsis = null;
      for (MSynopsis synopsis : candidateSynopses) {
        for (int i = 0; i < projs.size(); i++) {
          RexInputRef inputRef = (RexInputRef) projs.get(i);
          int newIndex = synopsis.getModelInstance().getColumnNames()
              .indexOf(parent.getRowType().getFieldNames().get(i));
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
        continue;
      }

      List<String> synopsisNames = new ArrayList<>();
      synopsisNames.add(tqn.get(0));
      synopsisNames.add(tqn.get(1));
      synopsisNames.add(bestSynopsis.getName());

      RelOptTableImpl synopsisTable = (RelOptTableImpl) planner.getTable(synopsisNames);
      JdbcTableScan newScan = new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
          (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());
      RelSubset subset = planner.register(newScan, null);

      LogicalProject newProject = new LogicalProject(
          parent.getCluster(), parent.getTraitSet(), ((Project) parent).getHints(),
          subset, newProjs, parent.getRowType());

      RelNode grandParent = getParent(aggregate, parent);
      RelSubset newSubset = planner.register(newProject, null);
      grandParent.replaceInput(0, newSubset);
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = true)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableApproxAggregateSynopsisRule.Config.of()
        .withOperandSupplier(b ->
            b.operand(LogicalAggregate.class)
                .predicate(ApproxAggregateSynopsisRule::isApproximateAggregate)
                .anyInputs());

    @Override default ApproxAggregateSynopsisRule toRule() {
      return new ApproxAggregateSynopsisRule(this);
    }
  }
}
