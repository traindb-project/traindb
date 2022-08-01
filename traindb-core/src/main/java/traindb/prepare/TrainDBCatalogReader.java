/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package traindb.prepare;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.function.Function;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Hints;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonikerImpl;
import org.apache.calcite.sql.validate.SqlMonikerType;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link Prepare.CatalogReader}
 * and also {@link SqlOperatorTable} based on tables and
 * functions defined schemas.
 */
public class TrainDBCatalogReader extends CalciteCatalogReader {

  public TrainDBCatalogReader(CalciteSchema rootSchema, List<String> defaultSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    this(rootSchema, SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
        defaultSchema.size() == 2
        ? ImmutableList.of(Objects.requireNonNull(defaultSchema, "defaultSchema"),
                           ImmutableList.of(defaultSchema.get(0)), ImmutableList.of())
        : ImmutableList.of(Objects.requireNonNull(defaultSchema, "defaultSchema"),
                           ImmutableList.of()),
        typeFactory, config);
  }

  protected TrainDBCatalogReader(CalciteSchema rootSchema,
                                 SqlNameMatcher nameMatcher, List<List<String>> schemaPaths,
                                 RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, nameMatcher, schemaPaths, typeFactory, config);
  }

  @Override public TrainDBCatalogReader withSchemaPath(List<String> schemaPath) {
    if (schemaPath.size() == 2) {
      return new TrainDBCatalogReader(rootSchema, nameMatcher,
          ImmutableList.of(schemaPath, ImmutableList.of(schemaPath.get(0)), ImmutableList.of()),
          typeFactory, config);
    }
    return new TrainDBCatalogReader(rootSchema, nameMatcher,
        ImmutableList.of(schemaPath, ImmutableList.of()), typeFactory, config);
  }

  @Override public Prepare.@Nullable PreparingTable getTable(final List<String> names) {
    return getTable(names, null);
  }

  public Prepare.@Nullable PreparingTable getTable(final List<String> names,
                                                   @Nullable Double rowCount) {
    // First look in the default schema, if any.
    // If not found, look in the root schema.
    CalciteSchema.TableEntry entry = SqlValidatorUtil.getTableEntry(this, names);
    if (entry != null) {
      final Table table = entry.getTable();
      if (table instanceof Wrapper) {
        final Prepare.PreparingTable relOptTable =
            ((Wrapper) table).unwrap(Prepare.PreparingTable.class);
        if (relOptTable != null) {
          return relOptTable;
        }
      }
      return RelOptTableImpl.create(this, table.getRowType(typeFactory), entry, rowCount);
    }
    return null;
  }

  @Override public CalciteConnectionConfig getConfig() {
    return config;
  }

  @Override public @Nullable RelDataType getNamedType(SqlIdentifier typeName) {
    CalciteSchema.TypeEntry typeEntry = SqlValidatorUtil.getTypeEntry(getRootSchema(), typeName);
    if (typeEntry != null) {
      return typeEntry.getType().apply(typeFactory);
    } else {
      return null;
    }
  }

  @Override public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    final CalciteSchema schema =
        SqlValidatorUtil.getSchema(rootSchema, names, nameMatcher);
    if (schema == null) {
      return ImmutableList.of();
    }
    final ImmutableList.Builder<SqlMoniker> result = new ImmutableList.Builder<>();

    // Add root schema if not anonymous
    if (!schema.name.equals("")) {
      result.add(moniker(schema, null, SqlMonikerType.SCHEMA));
    }

    final Map<String, CalciteSchema> schemaMap = schema.getSubSchemaMap();

    for (String subSchema : schemaMap.keySet()) {
      result.add(moniker(schema, subSchema, SqlMonikerType.SCHEMA));
    }

    for (String table : schema.getTableNames()) {
      result.add(moniker(schema, table, SqlMonikerType.TABLE));
    }

    final NavigableSet<String> functions = schema.getFunctionNames();
    for (String function : functions) { // views are here as well
      result.add(moniker(schema, function, SqlMonikerType.FUNCTION));
    }
    return result.build();
  }

  private static SqlMonikerImpl moniker(CalciteSchema schema, @Nullable String name,
      SqlMonikerType type) {
    final List<String> path = schema.path(name);
    if (path.size() == 1
        && !schema.root().name.equals("")
        && type == SqlMonikerType.SCHEMA) {
      type = SqlMonikerType.CATALOG;
    }
    return new SqlMonikerImpl(path, type);
  }

  @Override public Prepare.@Nullable PreparingTable getTableForMember(List<String> names) {
    return getTable(names);
  }

  @SuppressWarnings("deprecation")
  @Override public @Nullable RelDataTypeField field(RelDataType rowType, String alias) {
    return nameMatcher.field(rowType, alias);
  }

  @SuppressWarnings("deprecation")
  @Override public boolean matches(String string, String name) {
    return nameMatcher.matches(string, name);
  }

  @Override public RelDataType createTypeFromProjection(final RelDataType type,
      final List<String> columnNameList) {
    return SqlValidatorUtil.createTypeFromProjection(type, columnNameList,
        typeFactory, nameMatcher.isCaseSensitive());
  }

  /** Creates an operator table that contains functions in the given class
   * or classes.
   *
   * @see ModelHandler#addFunctions */
  public static SqlOperatorTable operatorTable(String... classNames) {
    // Dummy schema to collect the functions
    final CalciteSchema schema =
        CalciteSchema.createRootSchema(false, false);
    for (String className : classNames) {
      ModelHandler.addFunctions(schema.plus(), null, ImmutableList.of(),
          className, "*", true);
    }

    final ListSqlOperatorTable table = new ListSqlOperatorTable();
    for (String name : schema.getFunctionNames()) {
      schema.getFunctions(name, true).forEach(function -> {
        final SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
        table.add(toOp(id, function));
      });
    }
    return table;
  }

  /** Converts a function to a {@link SqlOperator}. */
  private static SqlOperator toOp(SqlIdentifier name,
      final org.apache.calcite.schema.Function function) {
    final Function<RelDataTypeFactory, List<RelDataType>> argTypesFactory =
        typeFactory -> function.getParameters()
            .stream()
            .map(o -> o.getType(typeFactory))
            .collect(Util.toImmutableList());
    final Function<RelDataTypeFactory, List<SqlTypeFamily>> typeFamiliesFactory =
        typeFactory -> argTypesFactory.apply(typeFactory)
            .stream()
            .map(type ->
                Util.first(type.getSqlTypeName().getFamily(),
                    SqlTypeFamily.ANY))
            .collect(Util.toImmutableList());
    final Function<RelDataTypeFactory, List<RelDataType>> paramTypesFactory =
        typeFactory ->
            argTypesFactory.apply(typeFactory)
                .stream()
                .map(type -> toSql(typeFactory, type))
                .collect(Util.toImmutableList());

    // Use a short-lived type factory to populate "typeFamilies" and "argTypes".
    // SqlOperandMetadata.paramTypes will use the real type factory, during
    // validation.
    final RelDataTypeFactory dummyTypeFactory = new JavaTypeFactoryImpl();
    final List<RelDataType> argTypes = argTypesFactory.apply(dummyTypeFactory);
    final List<SqlTypeFamily> typeFamilies =
        typeFamiliesFactory.apply(dummyTypeFactory);

    final SqlOperandTypeInference operandTypeInference =
        InferTypes.explicit(argTypes);

    final SqlOperandMetadata operandMetadata =
        OperandTypes.operandMetadata(typeFamilies, paramTypesFactory,
            i -> function.getParameters().get(i).getName(),
            i -> function.getParameters().get(i).isOptional());

    final SqlKind kind = kind(function);
    if (function instanceof ScalarFunction) {
      final SqlReturnTypeInference returnTypeInference =
          infer((ScalarFunction) function);
      return new SqlUserDefinedFunction(name, kind, returnTypeInference,
          operandTypeInference, operandMetadata, function);
    } else if (function instanceof AggregateFunction) {
      final SqlReturnTypeInference returnTypeInference =
          infer((AggregateFunction) function);
      return new SqlUserDefinedAggFunction(name, kind,
          returnTypeInference, operandTypeInference,
          operandMetadata, (AggregateFunction) function, false, false,
          Optionality.FORBIDDEN);
    } else if (function instanceof TableMacro) {
      return new SqlUserDefinedTableMacro(name, kind, ReturnTypes.CURSOR,
          operandTypeInference, operandMetadata, (TableMacro) function);
    } else if (function instanceof TableFunction) {
      return new SqlUserDefinedTableFunction(name, kind, ReturnTypes.CURSOR,
          operandTypeInference, operandMetadata, (TableFunction) function);
    } else {
      throw new AssertionError("unknown function type " + function);
    }
  }

  /** Deduces the {@link SqlKind} of a user-defined
   * function based on a {@link Hints} annotation, if present. */
  private static SqlKind kind(org.apache.calcite.schema.Function function) {
    if (function instanceof ScalarFunctionImpl) {
      Hints hints =
          ((ScalarFunctionImpl) function).method.getAnnotation(Hints.class);
      if (hints != null) {
        for (String hint : hints.value()) {
          if (hint.startsWith("SqlKind:")) {
            return SqlKind.valueOf(hint.substring("SqlKind:".length()));
          }
        }
      }
    }
    return SqlKind.OTHER_FUNCTION;
  }

  private static SqlReturnTypeInference infer(final ScalarFunction function) {
    return opBinding -> {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type;
      if (function instanceof ScalarFunctionImpl) {
        type = ((ScalarFunctionImpl) function).getReturnType(typeFactory,
            opBinding);
      } else {
        type = function.getReturnType(typeFactory);
      }
      return toSql(typeFactory, type);
    };
  }

  private static SqlReturnTypeInference infer(
      final AggregateFunction function) {
    return opBinding -> {
      final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      final RelDataType type = function.getReturnType(typeFactory);
      return toSql(typeFactory, type);
    };
  }

  private static RelDataType toSql(RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType
        && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
        == Object.class) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
    return JavaTypeFactoryImpl.toSql(typeFactory, type);
  }

  @Override public CalciteSchema getRootSchema() {
    return rootSchema;
  }

  @Override public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override public void registerRules(RelOptPlanner planner) {
  }

  @SuppressWarnings("deprecation")
  @Override public boolean isCaseSensitive() {
    return nameMatcher.isCaseSensitive();
  }

  @Override public SqlNameMatcher nameMatcher() {
    return nameMatcher;
  }

  @Override public <C extends Object> @Nullable C unwrap(Class<C> aClass) {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return null;
  }
}
