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

package traindb.planner;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.runtime.Hook;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TrainDBPlanner {

  private TrainDBPlanner() {
  }

  public static VolcanoPlanner createPlanner() {
    return createPlanner(null, null);
  }

  public static VolcanoPlanner createPlanner(Context externalContext) {
    return createPlanner(null, externalContext);
  }

  public static VolcanoPlanner createPlanner(@Nullable RelOptCostFactory costFactory,
                                             @Nullable Context externalContext) {
    VolcanoPlanner planner = new VolcanoPlanner(costFactory, externalContext);

    RelOptUtil.registerDefaultRules(planner, true, Hook.ENABLE_BINDABLE.get(false));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.setTopDownOpt(false);

    Hook.PLANNER.run(planner); // allow test to add or remove rules

    return planner;
  }
}
