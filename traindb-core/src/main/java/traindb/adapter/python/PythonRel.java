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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Python calling convention.
 */
public interface PythonRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("PYTHON", PythonRel.class);

  Result implement();

  class Result {

    public final PythonMLAggregateModel pyModel;

    public Result(PythonMLAggregateModel pyModel) {
      this.pyModel = pyModel;
    }
  }

}
