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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.catalog.pm.MModel;

/**
 * Implementation of {@link Aggregate} relational expression for Python ML.
 */
public class PythonMLAggregate extends Aggregate implements PythonRel {

  private static final Set<SqlKind> SUPPORTED_AGGREGATIONS =
      EnumSet.of(SqlKind.COUNT, SqlKind.AVG, SqlKind.SUM, SqlKind.STDDEV_POP, SqlKind.VAR_POP,
          SqlKind.ANY_VALUE);

  final MModel model;

  public PythonMLAggregate(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           RelNode input,
                           ImmutableBitSet groupSet,
                           List<ImmutableBitSet> groupSets,
                           List<AggregateCall> aggCalls,
                           MModel model) throws InvalidRelException {
    super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    this.model = model;

    assert this.groupSets.size() == 1 : "Grouping sets not supported";

    for (AggregateCall aggCall : aggCalls) {
      final SqlKind kind = aggCall.getAggregation().getKind();
      if (!SUPPORTED_AGGREGATIONS.contains(kind)) {
        final String message = String.format(Locale.ROOT,
            "Aggregation %s not supported (use one of %s)", kind, SUPPORTED_AGGREGATIONS);
        throw new InvalidRelException(message);
      }
    }

    if (getGroupType() != Group.SIMPLE) {
      final String message = String.format(Locale.ROOT, "Only %s grouping is supported. "
          + "Yours is %s", Group.SIMPLE, getGroupType());
      throw new InvalidRelException(message);
    }
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input,
                        ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                        List<AggregateCall> aggCalls) {
    try {
      return new PythonMLAggregate(getCluster(), traitSet, input,
          groupSet, groupSets, aggCalls, model);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(PythonToEnumerableConverterRule.INSTANCE);
  }

  @Override
  public Result implement() {
    Result result = ((PythonRel) getInput()).implement();
    return result;
  }

}
