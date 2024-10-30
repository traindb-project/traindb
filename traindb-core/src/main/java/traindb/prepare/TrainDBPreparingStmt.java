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
import static org.apache.calcite.linq4j.Nullness.castNonNull;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import edu.umd.cs.findbugs.annotations.OverrideMustInvoke;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.common.TrainDBLogger;
import traindb.engine.TrainDBListResultSet;
import traindb.planner.TrainDBPlanner;
import traindb.sql.calcite.TrainDBHintStrategyTable;

/** Holds state for the process of preparing a SQL statement. */
public class TrainDBPreparingStmt extends Prepare
    implements RelOptTable.ViewExpander {
  private TrainDBLogger LOG = TrainDBLogger.getLogger(this.getClass());
  protected final RelOptPlanner planner;
  protected final RexBuilder rexBuilder;
  protected final TrainDBPrepareImpl prepare;
  protected final CalciteSchema schema;
  protected final RelDataTypeFactory typeFactory;
  protected final SqlRexConvertletTable convertletTable;
  private final EnumerableRel.@Nullable Prefer prefer;
  private final RelOptCluster cluster;
  private final Map<String, Object> internalParameters =
      new LinkedHashMap<>();
  @SuppressWarnings("unused")
  private int expansionDepth;
  private @Nullable SqlValidator sqlValidator;

  TrainDBPreparingStmt(TrainDBPrepareImpl prepare,
                       CalcitePrepare.Context context,
                       Prepare.CatalogReader catalogReader,
                       RelDataTypeFactory typeFactory,
                       CalciteSchema schema,
                       EnumerableRel.@Nullable Prefer prefer,
                       RelOptCluster cluster,
                       Convention resultConvention,
                       SqlRexConvertletTable convertletTable) {
    super(context, catalogReader, resultConvention);
    this.prepare = prepare;
    this.schema = schema;
    this.prefer = prefer;
    this.cluster = cluster;
    this.planner = cluster.getPlanner();
    this.rexBuilder = cluster.getRexBuilder();
    this.typeFactory = typeFactory;
    this.convertletTable = convertletTable;
  }

  @Override protected void init(Class runtimeContextClass) {
  }

  public PreparedResult prepareQueryable(
      final Queryable queryable,
      RelDataType resultType) {
    return prepare_(() -> {
      final RelOptCluster cluster = prepare.createCluster(planner, rexBuilder);
      return new LixToRelTranslator(cluster, TrainDBPreparingStmt.this)
          .translate(queryable);
    }, resultType);
  }

  public PreparedResult prepareRel(final RelNode rel) {
    return prepare_(() -> rel, rel.getRowType());
  }

  private PreparedResult prepare_(Supplier<RelNode> fn,
                                  RelDataType resultType) {
    Class runtimeContextClass = Object.class;
    init(runtimeContextClass);

    final RelNode rel = fn.get();
    final RelDataType rowType = rel.getRowType();
    final List<Pair<Integer, String>> fields =
        Pair.zip(ImmutableIntList.identity(rowType.getFieldCount()),
            rowType.getFieldNames());
    final RelCollation collation =
        rel instanceof Sort
            ? ((Sort) rel).collation
            : RelCollations.EMPTY;
    RelRoot root = new RelRoot(rel, resultType, SqlKind.SELECT, fields,
        collation, new ArrayList<>());

    if (timingTracer != null) {
      timingTracer.traceTime("end sql2rel");
    }

    final RelDataType jdbcType =
        TrainDBPrepareImpl.makeStruct(rexBuilder.getTypeFactory(), resultType);
    fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null);
    parameterRowType = rexBuilder.getTypeFactory().builder().build();

    // Structured type flattening, view expansion, and plugging in
    // physical storage.
    root = root.withRel(flattenTypes(root.rel, true));

    // Trim unused fields.
    root = trimUnusedFields(root);

    final List<Materialization> materializations = ImmutableList.of();
    final List<CalciteSchema.LatticeEntry> lattices = ImmutableList.of();
    root = optimize(root, materializations, lattices);

    if (timingTracer != null) {
      timingTracer.traceTime("end optimization");
    }

    return implement(root);
  }

  @Override
  protected Program getProgram() {
    return ((TrainDBPlanner) planner).getProgram();
  }
  
  @Override
  public RelRoot optimize(RelRoot root,
  final List<Materialization> materializations,
  final List<CalciteSchema.LatticeEntry> lattices) {
    return super.optimize(root, materializations, lattices);
  }

  @Override
  public Prepare.PreparedResult prepareSql(
      SqlNode sqlQuery, SqlNode sqlNodeOriginal, Class runtimeContextClass,
      SqlValidator validator, boolean needsValidation) {
    init(runtimeContextClass);
    SqlToRelConverter.Config config =
        SqlToRelConverter.config()
            .withExpand(castNonNull(THREAD_EXPAND.get()))
            .withInSubQueryThreshold(castNonNull(THREAD_INSUBQUERY_THRESHOLD.get()))
            .withHintStrategyTable(TrainDBHintStrategyTable.HINT_STRATEGY_TABLE)
            .withExplain(sqlQuery.getKind() == SqlKind.EXPLAIN);
    Holder<SqlToRelConverter.Config> configHolder = Holder.of(config);
    Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.run(configHolder);
    SqlToRelConverter sqlToRelConverter =
        getSqlToRelConverter(validator, this.catalogReader, configHolder.get());

    SqlExplain sqlExplain = null;
    if (sqlQuery.getKind() == SqlKind.EXPLAIN) {
      sqlExplain = (SqlExplain)sqlQuery;
      sqlQuery = sqlExplain.getExplicandum();
      sqlToRelConverter.setDynamicParamCountInExplain(sqlExplain.getDynamicParamCount());
    }

    RelRoot root = sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true);
    Hook.CONVERTED.run(root.rel);
    if (this.timingTracer != null) {
      this.timingTracer.traceTime("end sql2rel");
    }

    RelDataType resultType = validator.getValidatedNodeType(sqlQuery);
    this.fieldOrigins = validator.getFieldOrigins(sqlQuery);
    assert this.fieldOrigins.size() == resultType.getFieldCount();

    this.parameterRowType = validator.getParameterRowType(sqlQuery);

    // Display logical plans before view expansion, plugging in physical
    // storage and decorrelation
    Hook.REL_BUILDER_SIMPLIFY.addThread(Hook.propertyJ(false));
    if (sqlExplain != null) {
      SqlExplain.Depth explainDepth = sqlExplain.getDepth();
      SqlExplainFormat format = sqlExplain.getFormat();
      SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
      switch(explainDepth) {
        case TYPE:
          return createPreparedExplanation(resultType, this.parameterRowType, null,
              format, detailLevel);
        case LOGICAL:
          return createPreparedExplanation(null, this.parameterRowType, root, format,
              detailLevel);
        default:
      }
    }
    LOG.debug(RelOptUtil.dumpPlan("Logical plan: ", root.rel, SqlExplainFormat.TEXT,
        SqlExplainLevel.ALL_ATTRIBUTES));

    // Structured type flattening, view expansion, and plugging in physical storage.
    root = root.withRel(this.flattenTypes(root.rel, true));

    if (this.context.config().forceDecorrelate()) {
      // Sub-query decorrelation.
      root = root.withRel(this.decorrelate(sqlToRelConverter, sqlQuery, root.rel));
    }

    if (sqlExplain != null) {
      switch(sqlExplain.getDepth()) {
        case PHYSICAL:
        default:
          root = this.optimize(root, this.getMaterializations(), this.getLattices());
          return this.createPreparedExplanation(null, this.parameterRowType, root,
              sqlExplain.getFormat(), sqlExplain.getDetailLevel());
      }
    }

    root = this.optimize(root, this.getMaterializations(), this.getLattices());

    if (this.timingTracer != null) {
      this.timingTracer.traceTime("end optimization");
    }

    // For transformation from DML -> DML, use result of rewrite
    // (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT),
    // use original kind.
    if (!root.kind.belongsTo(SqlKind.DML)) {
      root = root.withKind(sqlNodeOriginal.getKind());
    }
    return this.implement(root);
  }

  @Override protected SqlToRelConverter getSqlToRelConverter(
      SqlValidator validator,
      CatalogReader catalogReader,
      SqlToRelConverter.Config config) {
    return new SqlToRelConverter(this, validator, catalogReader, cluster,
        convertletTable, config);
  }

  @Override public RelNode flattenTypes(
      RelNode rootRel,
      boolean restructure) {
    final CalcitePrepare.SparkHandler spark = context.spark();
    if (spark.enabled()) {
      return spark.flattenTypes(planner, rootRel, restructure);
    }
    return rootRel;
  }

  @Override protected RelNode decorrelate(SqlToRelConverter sqlToRelConverter,
                                          SqlNode query, RelNode rootRel) {
    return sqlToRelConverter.decorrelate(query, rootRel);
  }

  @Override public RelRoot expandView(RelDataType rowType, String queryString,
                                      List<String> schemaPath, @Nullable List<String> viewPath) {
    expansionDepth++;

    SqlParser parser = prepare.createParser(queryString);
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseQuery();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }
    // View may have different schema path than current connection.
    final CatalogReader catalogReader =
        this.catalogReader.withSchemaPath(schemaPath);
    SqlValidator validator = createSqlValidator(catalogReader);
    final SqlToRelConverter.Config config =
        SqlToRelConverter.config().withTrimUnusedFields(true);
    SqlToRelConverter sqlToRelConverter =
        getSqlToRelConverter(validator, catalogReader, config);
    RelRoot root =
        sqlToRelConverter.convertQuery(sqlNode, true, true);

    --expansionDepth;
    return root;
  }

  protected SqlValidator createSqlValidator(CatalogReader catalogReader) {
    return TrainDBPrepareImpl.createSqlValidator(context,
        (TrainDBCatalogReader) catalogReader);
  }

  @Override protected SqlValidator getSqlValidator() {
    if (sqlValidator == null) {
      sqlValidator = createSqlValidator(catalogReader);
    }
    return sqlValidator;
  }

  @Override protected PreparedResult createPreparedExplanation(
      @Nullable RelDataType resultType,
      RelDataType parameterRowType,
      @Nullable RelRoot root,
      SqlExplainFormat format,
      SqlExplainLevel detailLevel) {
    return new CalcitePreparedExplain(resultType, parameterRowType, root, format, detailLevel);
  }

  @Override protected PreparedResult implement(RelRoot root) {
    Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);
    RelDataType resultType = root.rel.getRowType();
    boolean isDml = root.kind.belongsTo(SqlKind.DML);
    final Bindable bindable;
    if (resultConvention == BindableConvention.INSTANCE) {
      bindable = Interpreters.bindable(root.rel);
    } else {
      EnumerableRel enumerable = (EnumerableRel) root.rel;
      if (!root.isRefTrivial()) {
        final List<RexNode> projects = new ArrayList<>();
        final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
        for (int field : Pair.left(root.fields)) {
          projects.add(rexBuilder.makeInputRef(enumerable, field));
        }
        RexProgram program = RexProgram.create(enumerable.getRowType(),
            projects, null, root.validatedRowType, rexBuilder);
        enumerable = EnumerableCalc.create(enumerable, program);
      }

      LOG.debug(RelOptUtil.dumpPlan("Physical plan: ", enumerable, SqlExplainFormat.TEXT,
          SqlExplainLevel.ALL_ATTRIBUTES));

      try {
        CatalogReader.THREAD_LOCAL.set(catalogReader);
        final SqlConformance conformance = context.config().conformance();
        internalParameters.put("_conformance", conformance);
        bindable = EnumerableInterpretable.toBindable(internalParameters,
            context.spark(), enumerable,
            requireNonNull(prefer, "EnumerableRel.Prefer prefer"));
      } finally {
        CatalogReader.THREAD_LOCAL.remove();
      }
    }

    if (timingTracer != null) {
      timingTracer.traceTime("end codegen");
    }

    if (timingTracer != null) {
      timingTracer.traceTime("end compilation");
    }

    return new PreparedResultImpl(
        resultType,
        requireNonNull(parameterRowType, "parameterRowType"),
        requireNonNull(fieldOrigins, "fieldOrigins"),
        root.collation.getFieldCollations().isEmpty()
            ? ImmutableList.of()
            : ImmutableList.of(root.collation),
        root.rel,
        mapTableModOp(isDml, root.kind),
        isDml) {
      @Override public String getCode() {
        throw new UnsupportedOperationException();
      }

      @Override public Bindable getBindable(Meta.CursorFactory cursorFactory) {
        return bindable;
      }

      @Override public Type getElementType() {
        return ((Typed) bindable).getElementType();
      }
    };
  }

  @Override protected List<Materialization> getMaterializations() {
    final List<Prepare.Materialization> materializations =
        context.config().materializationsEnabled()
            ? MaterializationService.instance().query(schema)
            : ImmutableList.of();
    for (Prepare.Materialization materialization : materializations) {
      if (materialization instanceof TrainDBMaterialization) {
        prepare.populateMaterializations(context, cluster,
            (TrainDBMaterialization) materialization);
      } else {
        throw new RuntimeException("get materialization failed");
      }
    }
    return materializations;
  }

  @Override protected List<CalciteSchema.LatticeEntry> getLattices() {
    return Schemas.getLatticeEntries(schema);
  }

  protected Convention getResultConvention() {
    return resultConvention;
  }

  public Map<String, Object> getParameters() {
    return internalParameters;
  }

  /** An {@code EXPLAIN} statement, prepared and ready to execute. */
  private static class CalcitePreparedExplain extends Prepare.PreparedExplain {
    CalcitePreparedExplain(
        @Nullable RelDataType resultType,
        RelDataType parameterRowType,
        @Nullable RelRoot root,
        SqlExplainFormat format,
        SqlExplainLevel detailLevel) {
      super(resultType, parameterRowType, root, format, detailLevel);
    }

    @Override public Bindable getBindable(final Meta.CursorFactory cursorFactory) {
      final String explanation = getCode();
      return dataContext -> {
        switch (cursorFactory.style) {
          case ARRAY:
            return Linq4j.singletonEnumerable(new String[] {explanation});
          case OBJECT:
          default:
            return Linq4j.singletonEnumerable(explanation);
        }
      };
    }
  }

}
