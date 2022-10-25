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

public class TrainDBRules {

  private TrainDBRules() {
  }

  public static final ApproxAggregateSynopsisProjectScanRule
      APPROX_AGGREGATE_SYNOPSIS_PROJECT_SCAN =
      ApproxAggregateSynopsisProjectScanRule.Config.DEFAULT.toRule();

  public static final ApproxAggregateSynopsisFilterScanRule
          APPROX_AGGREGATE_SYNOPSIS_FILTER_SCAN =
          ApproxAggregateSynopsisFilterScanRule.Config.DEFAULT.toRule();

  public static final ApproxAggregateSynopsisAggregateScanRule
      APPROX_AGGREGATE_SYNOPSIS_AGGREGATE_SCAN =
      ApproxAggregateSynopsisAggregateScanRule.Config.DEFAULT.toRule();

  public static final ApproxAggregateInferenceRule
      APPROX_AGGREGATE_INFERENCE =
      ApproxAggregateInferenceRule.Config.DEFAULT.toRule();
}
