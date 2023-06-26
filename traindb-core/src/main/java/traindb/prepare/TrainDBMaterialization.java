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

package traindb.prepare;

import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.impl.StarTable;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TrainDBMaterialization extends Prepare.Materialization {

  final CalciteSchema.TableEntry materializedTable_;
  final String sql_;
  final List<String> viewSchemaPath_;

  @Nullable
  RelNode tableRel_;
  @Nullable
  RelNode queryRel_;
  @Nullable
  private RelOptTable starRelOptTable_;

  public TrainDBMaterialization(CalciteSchema.TableEntry materializedTable, String sql,
                                List<String> viewSchemaPath) {
    super(materializedTable, sql, viewSchemaPath);
    this.materializedTable_ = materializedTable;
    this.sql_ = sql;
    this.viewSchemaPath_ = viewSchemaPath;
  }

  @Override
  public void materialize(RelNode queryRel, RelOptTable starRelOptTable) {
    this.queryRel_ = queryRel;
    this.starRelOptTable_ = starRelOptTable;

    assert starRelOptTable.maybeUnwrap(StarTable.class).isPresent();
  }

}
