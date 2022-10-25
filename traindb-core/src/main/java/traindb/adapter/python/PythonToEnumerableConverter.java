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

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Relational expression representing a result table from a Python ML runner.
 */
public class PythonToEnumerableConverter extends ConverterImpl implements EnumerableRel {

  PythonToEnumerableConverter(RelNode input) {
    super(input.getCluster(), ConventionTraitDef.INSTANCE,
        input.getCluster().traitSetOf(EnumerableConvention.INSTANCE), input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PythonToEnumerableConverter(inputs.get(0));
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
    final PythonMLAggregateModel pyModel = ((PythonRel) input).implement().pyModel;
    String csvResultPath = pyModel.doInfer();

    try {
      BlockBuilder codeBlock = new BlockBuilder();
      DeclarationStatement fieldStmt =
          Expressions.declare(0, "fields", Expressions.new_(LinkedHashMap.class));
      codeBlock.add(fieldStmt);
      Method putMethod = Map.class.getMethod("put", Object.class, Object.class);
      for (RelDataTypeField f : getRowType().getFieldList()) {
        ConstantExpression fieldName = Expressions.constant(f.getName());
        ConstantExpression fieldType = Expressions.constant(f.getType().getSqlTypeName());
        MethodCallExpression callPut =
            Expressions.call(fieldStmt.parameter, putMethod, fieldName, fieldType);
        codeBlock.add(Expressions.statement(callPut));
      }

      Expression exprCsvResultPath =
          codeBlock.append("csvResultPath", Expressions.constant(csvResultPath, String.class));

      NewExpression pyEnumerable = Expressions.new_(PythonMLAggregateEnumerable.class,
          exprCsvResultPath, fieldStmt.parameter);
      codeBlock.add(Expressions.return_(null, pyEnumerable));
      PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
          prefer.prefer(JavaRowFormat.ARRAY));
      return implementor.result(physType, codeBlock.toBlock());
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

}
