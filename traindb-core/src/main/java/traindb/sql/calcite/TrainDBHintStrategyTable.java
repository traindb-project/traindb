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

package traindb.sql.calcite;

import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;

public class TrainDBHintStrategyTable {
  public static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategies();

  private static HintStrategyTable createHintStrategies() {
    return createHintStrategies(HintStrategyTable.builder());
  }

  static HintStrategyTable createHintStrategies(HintStrategyTable.Builder builder) {
    return builder
        .hintStrategy("APPROXIMATE_AGGR", HintPredicates.AGGREGATE)
        .hintStrategy("APPROXIMATE_AGGR_TABLE", HintPredicates.TABLE_SCAN)
        .build();
  }
}
