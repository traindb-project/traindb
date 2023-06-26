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

import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.type.ExtraSqlTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.catalog.CatalogContext;
import traindb.engine.TrainDBListResultSet;
import traindb.engine.TrainDBQueryEngine;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.planner.TrainDBPlanner;
import traindb.sql.TrainDBSql;
import traindb.sql.TrainDBSqlCommand;
import traindb.sql.calcite.TrainDBHintStrategyTable;
import traindb.sql.calcite.TrainDBSqlCalciteParserImpl;
import traindb.sql.fun.TrainDBSpatialOperatorTable;

public class TrainDBPrepareImpl extends CalcitePrepareImpl {

  /** Whether the bindable convention should be the root convention of any
   * plan. If not, enumerable convention is the default. */
  public final boolean enableBindable = Hook.ENABLE_BINDABLE.get(false);

  private TrainDBQueryEngine queryEngine;

  public TrainDBPrepareImpl(Context context) {
    queryEngine = new TrainDBQueryEngine(
        (TrainDBConnectionImpl) context.getDataContext().getQueryProvider());
  }

  @Override public ParseResult parse(
      Context context, String sql) {
    return parse_(context, sql, false, false, false);
  }

  @Override public ConvertResult convert(Context context, String sql) {
    return (ConvertResult) parse_(context, sql, true, false, false);
  }

  @Override public AnalyzeViewResult analyzeView(Context context, String sql, boolean fail) {
    return (AnalyzeViewResult) parse_(context, sql, true, true, fail);
  }

  /** Shared implementation for {@link #parse}, {@link #convert} and
   * {@link #analyzeView}. */
  private ParseResult parse_(Context context, String sql, boolean convert,
      boolean analyze, boolean fail) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    TrainDBCatalogReader catalogReader =
        new TrainDBCatalogReader(
            context.getRootSchema(),
            context.getDefaultSchemaPath(),
            typeFactory,
            context.config());
    SqlParser parser = createParser(sql);
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }
    final SqlValidator validator = createSqlValidator(context, catalogReader);
    SqlNode sqlNode1 = validator.validate(sqlNode);
    if (convert) {
      return convert_(
          context, sql, analyze, fail, catalogReader, validator, sqlNode1);
    }
    return new ParseResult(this, validator, sql, sqlNode1,
        validator.getValidatedNodeType(sqlNode1));
  }

  private ParseResult convert_(Context context, String sql, boolean analyze,
      boolean fail, TrainDBCatalogReader catalogReader, SqlValidator validator,
      SqlNode sqlNode1) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final Convention resultConvention =
        enableBindable ? BindableConvention.INSTANCE
            : EnumerableConvention.INSTANCE;

    // Use the Volcano because it can handle the traits.
    CatalogContext catalogContext =
        ((TrainDBConnectionImpl.TrainDBContextImpl) context).getCatalogContext();
    final VolcanoPlanner planner = new TrainDBPlanner(catalogContext, catalogReader);

    final SqlToRelConverter.Config config =
        SqlToRelConverter.config().withTrimUnusedFields(true)
            .withHintStrategyTable(TrainDBHintStrategyTable.HINT_STRATEGY_TABLE);

    final TrainDBPreparingStmt preparingStmt =
        new TrainDBPreparingStmt(this, context, catalogReader, typeFactory,
            context.getRootSchema(), null,
            createCluster(planner, new RexBuilder(typeFactory)),
            resultConvention, createConvertletTable());
    final SqlToRelConverter converter =
        preparingStmt.getSqlToRelConverter(validator, catalogReader, config);

    final RelRoot root = converter.convertQuery(sqlNode1, false, true);
    if (analyze) {
      return analyze_(validator, sql, sqlNode1, root, fail);
    }
    return new ConvertResult(this, validator, sql, sqlNode1,
        validator.getValidatedNodeType(sqlNode1), root);
  }

  private AnalyzeViewResult analyze_(SqlValidator validator, String sql,
      SqlNode sqlNode, RelRoot root, boolean fail) {
    final RexBuilder rexBuilder = root.rel.getCluster().getRexBuilder();
    RelNode rel = root.rel;
    final RelNode viewRel = rel;
    Project project;
    if (rel instanceof Project) {
      project = (Project) rel;
      rel = project.getInput();
    } else {
      project = null;
    }
    Filter filter;
    if (rel instanceof Filter) {
      filter = (Filter) rel;
      rel = filter.getInput();
    } else {
      filter = null;
    }
    TableScan scan;
    if (rel instanceof TableScan) {
      scan = (TableScan) rel;
    } else {
      scan = null;
    }
    if (scan == null) {
      if (fail) {
        throw validator.newValidationError(sqlNode,
            RESOURCE.modifiableViewMustBeBasedOnSingleTable());
      }
      return new AnalyzeViewResult(this, validator, sql, sqlNode,
          validator.getValidatedNodeType(sqlNode), root, null, null, null,
          null, false);
    }
    final RelOptTable targetRelTable = scan.getTable();
    final RelDataType targetRowType = targetRelTable.getRowType();
    final Table table = targetRelTable.unwrapOrThrow(Table.class);
    final List<String> tablePath = targetRelTable.getQualifiedName();
    List<Integer> columnMapping;
    final Map<Integer, RexNode> projectMap = new HashMap<>();
    if (project == null) {
      columnMapping = ImmutableIntList.range(0, targetRowType.getFieldCount());
    } else {
      columnMapping = new ArrayList<>();
      for (Ord<RexNode> node : Ord.zip(project.getProjects())) {
        if (node.e instanceof RexInputRef) {
          RexInputRef rexInputRef = (RexInputRef) node.e;
          int index = rexInputRef.getIndex();
          if (projectMap.get(index) != null) {
            if (fail) {
              throw validator.newValidationError(sqlNode,
                  RESOURCE.moreThanOneMappedColumn(
                      targetRowType.getFieldList().get(index).getName(),
                      Util.last(tablePath)));
            }
            return new AnalyzeViewResult(this, validator, sql, sqlNode,
                validator.getValidatedNodeType(sqlNode), root, null, null, null,
                null, false);
          }
          projectMap.put(index, rexBuilder.makeInputRef(viewRel, node.i));
          columnMapping.add(index);
        } else {
          columnMapping.add(-1);
        }
      }
    }
    final RexNode constraint;
    if (filter != null) {
      constraint = filter.getCondition();
    } else {
      constraint = rexBuilder.makeLiteral(true);
    }
    final List<RexNode> filters = new ArrayList<>();
    // If we put a constraint in projectMap above, then filters will not be empty despite
    // being a modifiable view.
    final List<RexNode> filters2 = new ArrayList<>();
    boolean retry = false;
    RelOptUtil.inferViewPredicates(projectMap, filters, constraint);
    if (fail && !filters.isEmpty()) {
      final Map<Integer, RexNode> projectMap2 = new HashMap<>();
      RelOptUtil.inferViewPredicates(projectMap2, filters2, constraint);
      if (!filters2.isEmpty()) {
        throw validator.newValidationError(sqlNode,
            RESOURCE.modifiableViewMustHaveOnlyEqualityPredicates());
      }
      retry = true;
    }

    // Check that all columns that are not projected have a constant value
    for (RelDataTypeField field : targetRowType.getFieldList()) {
      final int x = columnMapping.indexOf(field.getIndex());
      if (x >= 0) {
        assert Util.skip(columnMapping, x + 1).indexOf(field.getIndex()) < 0
            : "column projected more than once; should have checked above";
        continue; // target column is projected
      }
      if (projectMap.get(field.getIndex()) != null) {
        continue; // constant expression
      }
      if (field.getType().isNullable()) {
        continue; // don't need expression for nullable columns; NULL suffices
      }
      if (fail) {
        throw validator.newValidationError(sqlNode,
            RESOURCE.noValueSuppliedForViewColumn(field.getName(),
                Util.last(tablePath)));
      }
      return new AnalyzeViewResult(this, validator, sql, sqlNode,
          validator.getValidatedNodeType(sqlNode), root, null, null, null,
          null, false);
    }

    final boolean modifiable = filters.isEmpty() || retry && filters2.isEmpty();
    return new AnalyzeViewResult(this, validator, sql, sqlNode,
        validator.getValidatedNodeType(sqlNode), root, modifiable ? table : null,
        ImmutableList.copyOf(tablePath),
        constraint, ImmutableIntList.copyOf(columnMapping),
        modifiable);
  }

  @Override public void executeDdl(Context context, SqlNode node) {
    final CalciteConnectionConfig config = context.config();
    final SqlParserImplFactory parserFactory =
        config.parserFactory(
            SqlParserImplFactory.class, TrainDBSqlCalciteParserImpl.FACTORY);
    final DdlExecutor ddlExecutor = parserFactory.getDdlExecutor();
    ddlExecutor.executeDdl(context, node);
  }

  /** Factory method for default SQL parser. */
  protected SqlParser createParser(String sql) {
    return SqlParser.create(sql,
        SqlParser.config().withParserFactory(TrainDBSqlCalciteParserImpl.FACTORY));
  }

  /** Creates a query planner and initializes it with a default set of
   * rules. */
  protected RelOptPlanner createPlanner(
      final CalcitePrepare.Context prepareContext,
      org.apache.calcite.plan.@Nullable Context externalContext,
      @Nullable RelOptCostFactory costFactory) {
    if (externalContext == null) {
      externalContext = Contexts.of(prepareContext.config());
    }
    CatalogContext catalogContext =
        ((TrainDBConnectionImpl.TrainDBContextImpl) prepareContext).getCatalogContext();

    TrainDBCatalogReader catalogReader =
        new TrainDBCatalogReader(
            prepareContext.getRootSchema(),
            prepareContext.getDefaultSchemaPath(),
            prepareContext.getTypeFactory(),
            prepareContext.config());
    final VolcanoPlanner planner = new TrainDBPlanner(
        catalogContext, catalogReader, costFactory, externalContext);
    return planner;
  }

  @Override public <T> CalciteSignature<T> prepareQueryable(
      Context context,
      Queryable<T> queryable) {
    return prepare_(context, Query.of(queryable), queryable.getElementType(),
        -1);
  }

  @Override public <T> CalciteSignature<T> prepareSql(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount) {
    return prepare_(context, query, elementType, maxRowCount);
  }

  <T> CalciteSignature<T> prepare_(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    TrainDBCatalogReader catalogReader =
        new TrainDBCatalogReader(
            context.getRootSchema(),
            context.getDefaultSchemaPath(),
            typeFactory,
            context.config());
    final List<Function1<Context, RelOptPlanner>> plannerFactories =
        createPlannerFactories();
    if (plannerFactories.isEmpty()) {
      throw new AssertionError("no planner factories");
    }
    RuntimeException exception = Util.FoundOne.NULL;
    for (Function1<Context, RelOptPlanner> plannerFactory : plannerFactories) {
      final RelOptPlanner planner = plannerFactory.apply(context);
      if (planner == null) {
        throw new AssertionError("factory returned null planner");
      }
      try {
        return prepare2_(context, query, elementType, maxRowCount,
            catalogReader, planner);
      } catch (RelOptPlanner.CannotPlanException e) {
        exception = e;
      }
    }
    throw exception;
  }

  /**
   * Deduces the broad type of statement.
   * Currently returns SELECT for most statement types, but this may change.
   *
   * @param kind Kind of statement
   */
  private static Meta.StatementType getStatementType(SqlKind kind) {
    switch (kind) {
    case INSERT:
    case DELETE:
    case UPDATE:
      return Meta.StatementType.IS_DML;
    default:
      return Meta.StatementType.SELECT;
    }
  }

  /**
   * Deduces the broad type of statement for a prepare result.
   * Currently returns SELECT for most statement types, but this may change.
   *
   * @param preparedResult Prepare result
   */
  private static Meta.StatementType getStatementType(Prepare.PreparedResult preparedResult) {
    if (preparedResult.isDml()) {
      return Meta.StatementType.IS_DML;
    } else {
      return Meta.StatementType.SELECT;
    }
  }

  protected RelOptCluster createCluster(RelOptPlanner planner, RexBuilder rexBuilder) {
    return RelOptCluster.create(planner, rexBuilder);
  }

  CalciteSignature convertResultToSignature(Context context, String sql, TrainDBListResultSet res) {
    if (res.isEmpty()) {
      return new CalciteSignature<>(sql,
          ImmutableList.of(),
          ImmutableMap.of(), null,
          ImmutableList.of(), Meta.CursorFactory.OBJECT,
          null, ImmutableList.of(), -1, null,
          Meta.StatementType.OTHER_DDL);
    }

    List<AvaticaParameter> parameters = new ArrayList<>();
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (int i = 0; i < res.getColumnCount(); i++) {
      RelDataType type = context.getTypeFactory().createSqlType(
          Util.first(SqlTypeName.getNameForJdbcType(res.getColumnType(i)), SqlTypeName.ANY));
      parameters.add(
          new AvaticaParameter(
              false,
              getPrecision(type),
              getScale(type),
              getTypeOrdinal(type),
              getTypeName(type),
              getClassName(type),
              res.getColumnName(i)));
      columns.add(
          metaData(context.getTypeFactory(), res.getColumnType(i), res.getColumnName(i), type,
              type, null));
    }
    List<List<Object>> rows = new ArrayList<>();
    if (res.getRowCount() > 0) {
      while (res.next()) {
        List<Object> r = new ArrayList<>();
        for (int j = 0; j < res.getColumnCount(); j++) {
          r.add(res.getValue(j));
        }
        rows.add(r);
      }
    }

    return new CalciteSignature<>(sql,
        parameters,
        ImmutableMap.of(), null,
        columns, Meta.CursorFactory.ARRAY,
        context.getRootSchema(), ImmutableList.of(),
        res.getRowCount(),
        dataContext -> Linq4j.asEnumerable(rows),
        Meta.StatementType.SELECT);
  }

  <T> CalciteSignature<T> prepare2_(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount,
      TrainDBCatalogReader catalogReader,
      RelOptPlanner planner) {
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final EnumerableRel.Prefer prefer;
    if (elementType == Object[].class) {
      prefer = EnumerableRel.Prefer.ARRAY;
    } else {
      prefer = EnumerableRel.Prefer.CUSTOM;
    }
    final Convention resultConvention =
        enableBindable ? BindableConvention.INSTANCE
            : EnumerableConvention.INSTANCE;
    final TrainDBPreparingStmt preparingStmt =
        new TrainDBPreparingStmt(this, context, catalogReader, typeFactory,
            context.getRootSchema(), prefer, createCluster(planner, new RexBuilder(typeFactory)),
            resultConvention, createConvertletTable());

    final RelDataType x;
    final Prepare.PreparedResult preparedResult;
    final Meta.StatementType statementType;
    if (query.sql != null) {
      final CalciteConnectionConfig config = context.config();
      SqlParser.Config parserConfig = parserConfig()
          .withQuotedCasing(config.quotedCasing())
          .withUnquotedCasing(config.unquotedCasing())
          .withQuoting(config.quoting())
          .withConformance(config.conformance())
          .withCaseSensitive(config.caseSensitive());
      final SqlParserImplFactory parserFactory =
          config.parserFactory(SqlParserImplFactory.class, TrainDBSqlCalciteParserImpl.FACTORY);
      if (parserFactory != null) {
        parserConfig = parserConfig.withParserFactory(parserFactory);
      }

      // First check input query with TrainDB sql grammar
      List<TrainDBSqlCommand> commands = null;
      try {
        commands = TrainDBSql.parse(query.sql, parserConfig);
      } catch (Exception e) {
        if (commands != null) {
          commands.clear();
        }
      }
      TrainDBConnectionImpl conn =
          (TrainDBConnectionImpl) context.getDataContext().getQueryProvider();

      // INSERT QUERY LOGS
      LocalDateTime now = LocalDateTime.now();
      String currentTime = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
      String currentUser = conn.getProperties().getProperty("user");

      if (commands != null && commands.size() > 0) {
        try {
          // INSERT QUERY LOGS
          queryEngine.insertQueryLogs(currentTime, currentUser, query.sql);

          return convertResultToSignature(context, query.sql,
              TrainDBSql.run(commands.get(0), queryEngine));
        } catch (Exception e) {
          throw new RuntimeException(
              "failed to run statement: " + query + "\nerror msg: " + e.getMessage());
        } finally {
          try {
            queryEngine.insertTask();
          } catch ( Exception e) {
            throw new RuntimeException(
                    "failed to run statement: " + query + "\nerror msg: " + e.getMessage());
          }
        }
      }

      SqlParser parser = createParser(query.sql,  parserConfig);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseStmt();
        statementType = getStatementType(sqlNode.getKind());
      } catch (SqlParseException e) {
        throw new RuntimeException(
            "parse failed: " + e.getMessage(), e);
      }

      // INSERT QUERY LOGS
      try {
        queryEngine.insertQueryLogs(currentTime, currentUser, query.sql);
      } catch (Exception e) {
        throw new RuntimeException(
                "failed to query logging: " + query + "\nerror msg: " + e.getMessage());
      }

      Hook.PARSE_TREE.run(new Object[] {query.sql, sqlNode});

      if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
        executeDdl(context, sqlNode);

        return new CalciteSignature<>(query.sql,
            ImmutableList.of(),
            ImmutableMap.of(), null,
            ImmutableList.of(), Meta.CursorFactory.OBJECT,
            null, ImmutableList.of(), -1, null,
            Meta.StatementType.OTHER_DDL);
      }

      final SqlValidator validator =
          createSqlValidator(context, catalogReader);

      preparedResult = preparingStmt.prepareSql(
          sqlNode, Object.class, validator, true);
      switch (sqlNode.getKind()) {
      case INSERT:
      case DELETE:
      case UPDATE:
      case EXPLAIN:
        // FIXME: getValidatedNodeType is wrong for DML
        x = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
        break;
      default:
        x = validator.getValidatedNodeType(sqlNode);
      }
    } else if (query.queryable != null) {
      x = context.getTypeFactory().createType(elementType);
      preparedResult =
          preparingStmt.prepareQueryable(query.queryable, x);
      statementType = getStatementType(preparedResult);
    } else {
      assert query.rel != null;
      x = query.rel.getRowType();
      preparedResult = preparingStmt.prepareRel(query.rel);
      statementType = getStatementType(preparedResult);
    }

    final List<AvaticaParameter> parameters = new ArrayList<>();
    final RelDataType parameterRowType = preparedResult.getParameterRowType();
    for (RelDataTypeField field : parameterRowType.getFieldList()) {
      RelDataType type = field.getType();
      parameters.add(
          new AvaticaParameter(
              false,
              getPrecision(type),
              getScale(type),
              getTypeOrdinal(type),
              getTypeName(type),
              getClassName(type),
              field.getName()));
    }

    RelDataType jdbcType = makeStruct(typeFactory, x);
    final List<? extends @Nullable List<String>> originList = preparedResult.getFieldOrigins();
    final List<ColumnMetaData> columns =
        getColumnMetaDataList(typeFactory, x, jdbcType, originList);
    Class resultClazz = null;
    if (preparedResult instanceof Typed) {
      resultClazz = (Class) ((Typed) preparedResult).getElementType();
    }
    final Meta.CursorFactory cursorFactory =
        preparingStmt.getResultConvention() == BindableConvention.INSTANCE
            ? Meta.CursorFactory.ARRAY
            : Meta.CursorFactory.deduce(columns, resultClazz);
    //noinspection unchecked
    final Bindable<T> bindable = preparedResult.getBindable(cursorFactory);
    return new CalciteSignature<>(
        query.sql,
        parameters,
        preparingStmt.getParameters(),
        jdbcType,
        columns,
        cursorFactory,
        context.getRootSchema(),
        preparedResult instanceof TrainDBPreparedResultImpl
            ? ((TrainDBPreparedResultImpl) preparedResult).getCollations()
            : ImmutableList.of(),
        maxRowCount,
        bindable,
        statementType);
  }

  public static SqlValidator createSqlValidator(Context context,
      TrainDBCatalogReader catalogReader) {
    final SqlOperatorTable opTab0 =
        context.config().fun(SqlOperatorTable.class,
            SqlOperatorTables.chain(SqlStdOperatorTable.instance(),
                TrainDBSpatialOperatorTable.instance()));
    final List<SqlOperatorTable> list = new ArrayList<>();
    list.add(opTab0);
    list.add(catalogReader);
    final SqlOperatorTable opTab = SqlOperatorTables.chain(list);
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final CalciteConnectionConfig connectionConfig = context.config();
    final SqlValidator.Config config = SqlValidator.Config.DEFAULT
        .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
        .withConformance(connectionConfig.conformance())
        .withDefaultNullCollation(connectionConfig.defaultNullCollation())
        .withIdentifierExpansion(true);
    return new CalciteSqlValidator(opTab, catalogReader, typeFactory,
        config);
  }

  private static List<ColumnMetaData> getColumnMetaDataList(
      JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType,
      List<? extends @Nullable List<String>> originList) {
    final List<ColumnMetaData> columns = new ArrayList<>();
    for (Ord<RelDataTypeField> pair : Ord.zip(jdbcType.getFieldList())) {
      final RelDataTypeField field = pair.e;
      final RelDataType type = field.getType();
      final RelDataType fieldType =
          x.isStruct() ? x.getFieldList().get(pair.i).getType() : type;
      columns.add(
          metaData(typeFactory, columns.size(), field.getName(), type,
              fieldType, originList.get(pair.i)));
    }
    return columns;
  }

  private static ColumnMetaData metaData(JavaTypeFactory typeFactory, int ordinal,
      String fieldName, RelDataType type, @Nullable RelDataType fieldType,
      @Nullable List<String> origins) {
    final ColumnMetaData.AvaticaType avaticaType =
        avaticaType(typeFactory, type, fieldType);
    return new ColumnMetaData(
        ordinal,
        false,
        true,
        false,
        false,
        type.isNullable()
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true,
        type.getPrecision(),
        fieldName,
        origin(origins, 0),
        origin(origins, 2),
        getPrecision(type),
        getScale(type),
        origin(origins, 1),
        null,
        avaticaType,
        true,
        false,
        false,
        avaticaType.columnClassName());
  }

  private static ColumnMetaData.AvaticaType avaticaType(JavaTypeFactory typeFactory,
      RelDataType type, @Nullable RelDataType fieldType) {
    final String typeName = getTypeName(type);
    if (type.getComponentType() != null) {
      final ColumnMetaData.AvaticaType componentType =
          avaticaType(typeFactory, type.getComponentType(), null);
      final Type clazz = typeFactory.getJavaClass(type.getComponentType());
      final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
      assert rep != null;
      return ColumnMetaData.array(componentType, typeName, rep);
    } else {
      int typeOrdinal = getTypeOrdinal(type);
      switch (typeOrdinal) {
      case Types.STRUCT:
        final List<ColumnMetaData> columns = new ArrayList<>(type.getFieldList().size());
        for (RelDataTypeField field : type.getFieldList()) {
          columns.add(
              metaData(typeFactory, field.getIndex(), field.getName(),
                  field.getType(), null, null));
        }
        return ColumnMetaData.struct(columns);
      case ExtraSqlTypes.GEOMETRY:
        typeOrdinal = Types.VARCHAR;
        // fall through
      default:
        final Type clazz =
            typeFactory.getJavaClass(Util.first(fieldType, type));
        final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
        assert rep != null;
        return ColumnMetaData.scalar(typeOrdinal, typeName, rep);
      }
    }
  }

  private static @Nullable String origin(@Nullable List<String> origins, int offsetFromEnd) {
    return origins == null || offsetFromEnd >= origins.size()
        ? null
        : origins.get(origins.size() - 1 - offsetFromEnd);
  }

  private static int getTypeOrdinal(RelDataType type) {
    return type.getSqlTypeName().getJdbcOrdinal();
  }

  private static String getClassName(@SuppressWarnings("unused") RelDataType type) {
    return Object.class.getName(); // CALCITE-2613
  }

  private static int getScale(RelDataType type) {
    return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
        ? 0
        : type.getScale();
  }

  private static int getPrecision(RelDataType type) {
    return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
        ? 0
        : type.getPrecision();
  }

  /** Returns the type name in string form. Does not include precision, scale
   * or whether nulls are allowed. Example: "DECIMAL" not "DECIMAL(7, 2)";
   * "INTEGER" not "JavaType(int)". */
  private static String getTypeName(RelDataType type) {
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    switch (sqlTypeName) {
    case ARRAY:
    case MULTISET:
    case MAP:
    case ROW:
      return type.toString(); // e.g. "INTEGER ARRAY"
    case INTERVAL_YEAR_MONTH:
      return "INTERVAL_YEAR_TO_MONTH";
    case INTERVAL_DAY_HOUR:
      return "INTERVAL_DAY_TO_HOUR";
    case INTERVAL_DAY_MINUTE:
      return "INTERVAL_DAY_TO_MINUTE";
    case INTERVAL_DAY_SECOND:
      return "INTERVAL_DAY_TO_SECOND";
    case INTERVAL_HOUR_MINUTE:
      return "INTERVAL_HOUR_TO_MINUTE";
    case INTERVAL_HOUR_SECOND:
      return "INTERVAL_HOUR_TO_SECOND";
    case INTERVAL_MINUTE_SECOND:
      return "INTERVAL_MINUTE_TO_SECOND";
    default:
      return sqlTypeName.getName(); // e.g. "DECIMAL", "INTERVAL_YEAR_MONTH"
    }
  }

  protected void populateMaterializations(Context context,
      RelOptCluster cluster, TrainDBMaterialization materialization) {
    // REVIEW: initialize queryRel and tableRel inside MaterializationService,
    // not here?
    try {
      final CalciteSchema schema = materialization.materializedTable_.schema;
      TrainDBCatalogReader catalogReader =
          new TrainDBCatalogReader(
              schema.root(),
              materialization.viewSchemaPath_,
              context.getTypeFactory(),
              context.config());
      final CalciteMaterializer materializer =
          new CalciteMaterializer(this, context, catalogReader, schema,
              cluster, createConvertletTable());
      materializer.populate(materialization);
    } catch (Exception e) {
      throw new RuntimeException("While populating materialization "
          + materialization.materializedTable_.path(), e);
    }
  }

  static RelDataType makeStruct(
      RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type.isStruct()) {
      return type;
    }
    return typeFactory.builder().add("$0", type).build();
  }

  @Deprecated // to be removed before 2.0
  public <R> R perform(CalciteServerStatement statement,
      Frameworks.PrepareAction<R> action) {
    return perform(statement, action.getConfig(), action);
  }

  /** Executes a prepare action. */
  public <R> R perform(CalciteServerStatement statement,
      FrameworkConfig config, Frameworks.BasePrepareAction<R> action) {
    final CalcitePrepare.Context prepareContext =
        statement.createPrepareContext();
    final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
    SchemaPlus defaultSchema = config.getDefaultSchema();
    final CalciteSchema schema =
        defaultSchema != null
            ? CalciteSchema.from(defaultSchema)
            : prepareContext.getRootSchema();
    TrainDBCatalogReader catalogReader =
        new TrainDBCatalogReader(schema.root(),
            schema.path(null),
            typeFactory,
            prepareContext.config());
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptPlanner planner =
        createPlanner(prepareContext,
            config.getContext(),
            config.getCostFactory());
    final RelOptCluster cluster = createCluster(planner, rexBuilder);
    return action.apply(cluster, catalogReader,
        prepareContext.getRootSchema().plus(), statement);
  }

  public static class TrainDBPreparedResultImpl extends Prepare.PreparedResultImpl {

    protected TrainDBPreparedResultImpl(RelDataType rowType, RelDataType parameterRowType,
                                        List<? extends List<String>> fieldOrigins,
                                        List<RelCollation> collations,
                                        RelNode rootRel,
                                        TableModify.Operation tableModOp,
                                        boolean isDml) {
      super(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml);

    }

    @Override
    public Type getElementType() {
      return null;
    }

    @Override
    public String getCode() {
      return null;
    }

    @Override
    public Bindable getBindable(Meta.CursorFactory cursorFactory) {
      return null;
    }

    public List<RelCollation> getCollations() {
      return collations;
    }
  }

}
