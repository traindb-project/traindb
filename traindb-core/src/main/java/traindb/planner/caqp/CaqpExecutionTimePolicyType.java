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

package traindb.planner.caqp;

import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Policy for approximate query processing with an execution time constraint
 * ('WITHIN n SECONDS' clause).
 */
public enum CaqpExecutionTimePolicyType {

  ROW("row", CaqpExecutionTimeRowPolicy.class, 10000000);

  private static final String DEFAULT_CAQP_EXECUTION_TIME_POLICY = "row";
  private static final Map<String, CaqpExecutionTimePolicyType> NAME_TO_POLICY_MAP =
      new HashMap<>();
  private static final Map<Class<? extends CaqpExecutionTimePolicy>, CaqpExecutionTimePolicyType>
      CLASS_TO_POLICY_MAP = new HashMap<>();

  static {
    CaqpExecutionTimePolicyType[] values = CaqpExecutionTimePolicyType.values();
    for (CaqpExecutionTimePolicyType value : values) {
      NAME_TO_POLICY_MAP.put(value.name, value);
      CLASS_TO_POLICY_MAP.put(value.policyClass, value);
    }
  }

  public final String name;
  public final Class<? extends CaqpExecutionTimePolicy> policyClass;
  public final double defaultUnitAmount;

  CaqpExecutionTimePolicyType(String name, Class<? extends CaqpExecutionTimePolicy> policyClass,
                              double defaultUnitAmount) {
    this.name = name;
    this.policyClass = policyClass;
    this.defaultUnitAmount = defaultUnitAmount;
  }

  public static @Nullable CaqpExecutionTimePolicyType of(String name) {
    return NAME_TO_POLICY_MAP.get(name);
  }

  public static CaqpExecutionTimePolicyType getDefaultPolicy() {
    return NAME_TO_POLICY_MAP.get(DEFAULT_CAQP_EXECUTION_TIME_POLICY);
  }

}
