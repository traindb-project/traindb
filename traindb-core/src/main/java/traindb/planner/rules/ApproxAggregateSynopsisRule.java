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

import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

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

  //~ Methods ----------------------------------------------------------------
  @Override public void onMatch(RelOptRuleCall call) {
    // TODO replace target base table to its corresponding synopsis
    return;
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
