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

package traindb.adapter.python;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PythonMLAggregateModelScan extends TableScan implements PythonRel {

  PythonMLAggregateModel pyModel;

  public PythonMLAggregateModelScan(RelOptCluster cluster, RelTraitSet traitSet,
                                    RelOptTable table, PythonMLAggregateModel pyModel) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.pyModel = pyModel;

    assert getConvention() == PythonRel.CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.01);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(PythonToEnumerableConverterRule.INSTANCE);
  }

  @Override
  public Result implement() {
    return new Result(pyModel);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("model", pyModel.runner.getModelName());
  }
}
