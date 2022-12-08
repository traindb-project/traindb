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

import traindb.catalog.pm.MSynopsis;

public class CaqpExecutionTimeRowPolicy extends CaqpExecutionTimePolicy {

  public CaqpExecutionTimeRowPolicy(double unitAmount) {
    super(unitAmount);
  }

  public boolean check(MSynopsis synopsis, double time) {
    if (synopsis.getRows() > (time * unitAmount))   {
      return false;
    }
    return true;
  }

}
