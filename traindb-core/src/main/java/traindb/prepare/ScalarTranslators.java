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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.PseudoField;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class ScalarTranslators {

  /** Translator from Java AST to {@link RexNode}. */
  interface ScalarTranslator {
    RexNode toRex(BlockStatement statement);
    List<RexNode> toRexList(BlockStatement statement);
    RexNode toRex(Expression expression);
    ScalarTranslator bind(List<ParameterExpression> parameterList,
                          List<RexNode> values);
  }

  /** Basic translator. */
  static class EmptyScalarTranslator implements ScalarTranslator {
    private final RexBuilder rexBuilder;

    EmptyScalarTranslator(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public static ScalarTranslator empty(RexBuilder builder) {
      return new EmptyScalarTranslator(builder);
    }

    @Override public List<RexNode> toRexList(BlockStatement statement) {
      final List<Expression> simpleList = simpleList(statement);
      final List<RexNode> list = new ArrayList<>();
      for (Expression expression1 : simpleList) {
        list.add(toRex(expression1));
      }
      return list;
    }

    @Override public RexNode toRex(BlockStatement statement) {
      return toRex(Blocks.simple(statement));
    }

    private static List<Expression> simpleList(BlockStatement statement) {
      Expression simple = Blocks.simple(statement);
      if (simple instanceof NewExpression) {
        NewExpression newExpression = (NewExpression) simple;
        return newExpression.arguments;
      } else {
        return Collections.singletonList(simple);
      }
    }

    @Override public RexNode toRex(Expression expression) {
      switch (expression.getNodeType()) {
        case MemberAccess:
          // Case-sensitive name match because name was previously resolved.
          MemberExpression memberExpression = (MemberExpression) expression;
          PseudoField field = memberExpression.field;
          Expression targetExpression = requireNonNull(memberExpression.expression,
              () -> "static field access is not implemented yet."
                  + " field.name=" + field.getName()
                  + ", field.declaringClass=" + field.getDeclaringClass());
          return rexBuilder.makeFieldAccess(
              toRex(targetExpression),
              field.getName(),
              true);
        case GreaterThan:
          return binary(expression, SqlStdOperatorTable.GREATER_THAN);
        case LessThan:
          return binary(expression, SqlStdOperatorTable.LESS_THAN);
        case Parameter:
          return parameter((ParameterExpression) expression);
        case Call:
          MethodCallExpression call = (MethodCallExpression) expression;
          SqlOperator operator =
              RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get(call.method);
          if (operator != null) {
            return rexBuilder.makeCall(
                type(call),
                operator,
                toRex(
                    Expressions.<Expression>list()
                        .appendIfNotNull(call.targetExpression)
                        .appendAll(call.expressions)));
          }
          throw new RuntimeException(
              "Could translate call to method " + call.method);
        case Constant:
          final ConstantExpression constant =
              (ConstantExpression) expression;
          Object value = constant.value;
          if (value instanceof Number) {
            Number number = (Number) value;
            if (value instanceof Double || value instanceof Float) {
              return rexBuilder.makeApproxLiteral(
                  BigDecimal.valueOf(number.doubleValue()));
            } else if (value instanceof BigDecimal) {
              return rexBuilder.makeExactLiteral((BigDecimal) value);
            } else {
              return rexBuilder.makeExactLiteral(
                  BigDecimal.valueOf(number.longValue()));
            }
          } else if (value instanceof Boolean) {
            return rexBuilder.makeLiteral((Boolean) value);
          } else {
            return rexBuilder.makeLiteral(constant.toString());
          }
        default:
          throw new UnsupportedOperationException(
              "unknown expression type " + expression.getNodeType() + " "
                  + expression);
      }
    }

    private RexNode binary(Expression expression, SqlBinaryOperator op) {
      BinaryExpression call = (BinaryExpression) expression;
      return rexBuilder.makeCall(type(call), op,
          toRex(ImmutableList.of(call.expression0, call.expression1)));
    }

    private List<RexNode> toRex(List<Expression> expressions) {
      final List<RexNode> list = new ArrayList<>();
      for (Expression expression : expressions) {
        list.add(toRex(expression));
      }
      return list;
    }

    protected RelDataType type(Expression expression) {
      final Type type = expression.getType();
      return ((JavaTypeFactory) rexBuilder.getTypeFactory()).createType(type);
    }

    @Override public ScalarTranslator bind(
        List<ParameterExpression> parameterList, List<RexNode> values) {
      return new LambdaScalarTranslator(
          rexBuilder, parameterList, values);
    }

    public RexNode parameter(ParameterExpression param) {
      throw new RuntimeException("unknown parameter " + param);
    }
  }

  /** Translator that looks for parameters. */
  private static class LambdaScalarTranslator extends EmptyScalarTranslator {
    private final List<ParameterExpression> parameterList;
    private final List<RexNode> values;

    LambdaScalarTranslator(
        RexBuilder rexBuilder,
        List<ParameterExpression> parameterList,
        List<RexNode> values) {
      super(rexBuilder);
      this.parameterList = parameterList;
      this.values = values;
    }

    @Override public RexNode parameter(ParameterExpression param) {
      int i = parameterList.indexOf(param);
      if (i >= 0) {
        return values.get(i);
      }
      throw new RuntimeException("unknown parameter " + param);
    }
  }

}
