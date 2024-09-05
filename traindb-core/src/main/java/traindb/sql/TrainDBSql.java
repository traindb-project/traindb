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

package traindb.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParser;
import traindb.common.TrainDBLogger;
import traindb.engine.TrainDBListResultSet;

public final class TrainDBSql {

  private TrainDBSql() {
  }

  public static List<TrainDBSqlCommand> parse(String query, SqlParser.Config parserConfig) {
    ANTLRInputStream input = new ANTLRInputStream(query);
    TrainDBSqlLexer lexer = new TrainDBSqlLexer(input, parserConfig);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    TrainDBSqlParser parser = new TrainDBSqlParser(tokens);
    TrainDBErrorListener trainDBErrorListener = new TrainDBErrorListener();

    // remove default console output printing error listener
    // to suppress syntax error messages for TrainDB SQL Parser
    lexer.removeErrorListener(ConsoleErrorListener.INSTANCE);
    lexer.addErrorListener(trainDBErrorListener);
    parser.removeErrorListener(ConsoleErrorListener.INSTANCE);
    parser.addErrorListener(trainDBErrorListener);

    ParserRuleContext tree = parser.traindbStmts();
    ParseTreeWalker walker = new ParseTreeWalker();
    Listener lsnr = new Listener();
    walker.walk(lsnr, tree);

    return lsnr.getSqlCommands();
  }

  public static TrainDBListResultSet run(TrainDBSqlCommand command, TrainDBSqlRunner runner)
      throws Exception {
    switch (command.getType()) {
      case CREATE_MODELTYPE:
        TrainDBSqlCreateModeltype createModeltype = (TrainDBSqlCreateModeltype) command;
        runner.createModeltype(createModeltype.getName(), createModeltype.getCategory(),
            createModeltype.getLocation(), createModeltype.getClassName(),
            createModeltype.getUri());
        break;
      case DROP_MODELTYPE:
        TrainDBSqlDropModeltype dropModeltype = (TrainDBSqlDropModeltype) command;
        runner.dropModeltype(dropModeltype.getName());
        break;
      case SHOW_MODELTYPES:
        TrainDBSqlShowCommand showModeltypes = (TrainDBSqlShowCommand) command;
        return runner.showModeltypes(showModeltypes.getWhereExpressionMap());
      case SHOW_MODELS:
        TrainDBSqlShowCommand showModels = (TrainDBSqlShowCommand) command;
        return runner.showModels(showModels.getWhereExpressionMap());
      case TRAIN_MODEL:
        TrainDBSqlTrainModel trainModel = (TrainDBSqlTrainModel) command;
        runner.trainModel(
            trainModel.getModeltypeName(), trainModel.getModelName(),
            trainModel.getSchemaNames().get(0), trainModel.getTableNames().get(0),
            trainModel.getColumnNames().get(0), trainModel.getSamplePercent(),
            trainModel.getTrainOptions());
        break;
      case DROP_MODEL:
        TrainDBSqlDropModel dropModel = (TrainDBSqlDropModel) command;
        runner.dropModel(dropModel.getModelName());
        break;
      case CREATE_SYNOPSIS:
        TrainDBSqlCreateSynopsis createSynopsis = (TrainDBSqlCreateSynopsis) command;
        runner.createSynopsis(createSynopsis.getSynopsisName(), createSynopsis.getSynopsisType(),
            createSynopsis.getModelName(), createSynopsis.getLimitRows(),
            createSynopsis.getLimitPercent());
        break;
      case DROP_SYNOPSIS:
        TrainDBSqlDropSynopsis dropSynopsis = (TrainDBSqlDropSynopsis) command;
        runner.dropSynopsis(dropSynopsis.getSynopsisName());
        break;
      case SHOW_SYNOPSES:
        TrainDBSqlShowCommand showSynopses = (TrainDBSqlShowCommand) command;
        return runner.showSynopses(showSynopses.getWhereExpressionMap());
      case SHOW_SCHEMAS:
        TrainDBSqlShowCommand showSchemas = (TrainDBSqlShowCommand) command;
        return runner.showSchemas(showSchemas.getWhereExpressionMap());
      case SHOW_TABLES:
        TrainDBSqlShowCommand showTables = (TrainDBSqlShowCommand) command;
        return runner.showTables(showTables.getWhereExpressionMap());
      case SHOW_COLUMNS:
        TrainDBSqlShowCommand showColumns = (TrainDBSqlShowCommand) command;
        return runner.showColumns(showColumns.getWhereExpressionMap());
      case SHOW_HYPERPARAMETERS:
        TrainDBSqlShowCommand showHyperparams = (TrainDBSqlShowCommand) command;
        return runner.showHyperparameters(showHyperparams.getWhereExpressionMap());
      case SHOW_TRAININGS:
        TrainDBSqlShowCommand showTrainings = (TrainDBSqlShowCommand) command;
        return runner.showTrainings(showTrainings.getWhereExpressionMap());
      case SHOW_PARTITIONS:
        TrainDBSqlShowCommand showPartitions = (TrainDBSqlShowCommand) command;
        return runner.showPartitions(showPartitions.getWhereExpressionMap());
      case USE_SCHEMA:
        TrainDBSqlUseSchema useSchema = (TrainDBSqlUseSchema) command;
        runner.useSchema(useSchema.getSchemaName());
        break;
      case DESCRIBE_TABLE:
        TrainDBSqlDescribeTable describeTable = (TrainDBSqlDescribeTable) command;
        return runner.describeTable(describeTable.getSchemaName(), describeTable.getTableName());
      case BYPASS_DDL_STMT:
        TrainDBSqlBypassDdlStmt bypassDdlStmt = (TrainDBSqlBypassDdlStmt) command;
        runner.bypassDdlStmt(bypassDdlStmt.getStatement());
        break;
      case INCREMENTAL_QUERY:
      case INCREMENTAL_PARALLEL_QUERY:
        break;
      case SHOW_QUERY_LOGS:
        TrainDBSqlShowCommand showQueryLogs = (TrainDBSqlShowCommand) command;
        return runner.showQueryLogs(showQueryLogs.getWhereExpressionMap());
      case SHOW_TASKS:
        TrainDBSqlShowCommand showTasks = (TrainDBSqlShowCommand) command;
        return runner.showTasks(showTasks.getWhereExpressionMap());
      case DELETE_QUERY_LOGS:
        TrainDBSqlDeleteQueryLogs deleteQueryLogs = (TrainDBSqlDeleteQueryLogs) command;
        runner.deleteQueryLogs(deleteQueryLogs.getRowCount());
        break;
      case DELETE_TASKS:
        TrainDBSqlDeleteTasks deleteTasks = (TrainDBSqlDeleteTasks) command;
        runner.deleteTasks(deleteTasks.getRowCount());
        break;
      case EXPORT_MODEL:
        TrainDBSqlExportModel exportModel = (TrainDBSqlExportModel) command;
        return runner.exportModel(exportModel.getModelName(), exportModel.getExportFilename());
      case IMPORT_MODEL:
        TrainDBSqlImportModel importModel = (TrainDBSqlImportModel) command;
        return runner.importModel(importModel.getModelName(), importModel.getModelBinaryString(),
            importModel.getImportFilename());
      case ALTER_MODEL_RENAME: {
        TrainDBSqlAlterModel alterModel = (TrainDBSqlAlterModel) command;
        runner.renameModel(alterModel.getModelName(), alterModel.getNewModelName());
        break;
      }
      case ALTER_MODEL_ENABLE: {
        TrainDBSqlAlterModel alterModel = (TrainDBSqlAlterModel) command;
        runner.enableModel(alterModel.getModelName());
        break;
      }
      case ALTER_MODEL_DISABLE: {
        TrainDBSqlAlterModel alterModel = (TrainDBSqlAlterModel) command;
        runner.disableModel(alterModel.getModelName());
        break;
      }
      case EXPORT_SYNOPSIS:
        TrainDBSqlExportSynopsis exportSynopsis = (TrainDBSqlExportSynopsis) command;
        return runner.exportSynopsis(exportSynopsis.getSynopsisName(), exportSynopsis.getExportFilename());
      case IMPORT_SYNOPSIS:
        TrainDBSqlImportSynopsis importSynopsis = (TrainDBSqlImportSynopsis) command;
        return runner.importSynopsis(importSynopsis.getSynopsisName(),
            importSynopsis.getSynopsisType(), importSynopsis.getSynopsisBinaryString(),
            importSynopsis.getImportFilename());
      case ALTER_SYNOPSIS_RENAME: {
        TrainDBSqlAlterSynopsis alterSynopsis = (TrainDBSqlAlterSynopsis) command;
        runner.renameSynopsis(alterSynopsis.getSynopsisName(), alterSynopsis.getNewSynopsisName());
        break;
      }
      case ALTER_SYNOPSIS_ENABLE: {
        TrainDBSqlAlterSynopsis alterSynopsis = (TrainDBSqlAlterSynopsis) command;
        runner.enableSynopsis(alterSynopsis.getSynopsisName());
        break;
      }
      case ALTER_SYNOPSIS_DISABLE: {
        TrainDBSqlAlterSynopsis alterSynopsis = (TrainDBSqlAlterSynopsis) command;
        runner.disableSynopsis(alterSynopsis.getSynopsisName());
        break;
      }
      case ANALYZE_SYNOPSIS:
        TrainDBSqlAnalyzeSynopsis analyzeSynopsis = (TrainDBSqlAnalyzeSynopsis) command;
        runner.analyzeSynopsis(analyzeSynopsis.getSynopsisName());
        break;
      default:
        throw new RuntimeException("invalid TrainDB SQL command");
    }
    return TrainDBListResultSet.empty();
  }

  public static String toCase(String s, Casing casing) {
    switch (casing) {
      case TO_UPPER:
        return s.toUpperCase();
      case TO_LOWER:
        return s.toLowerCase();
      default:
        return s;
    }
  }

  private static class Listener extends TrainDBSqlBaseListener {

    private static final TrainDBLogger LOG =
        TrainDBLogger.getLogger("traindb.sql.TrainDBSql.Listener");

    private final List<TrainDBSqlCommand> commands;

    Listener() {
      commands = new ArrayList<>();
    }

    @Override
    public void exitTraindbStmts(TrainDBSqlParser.TraindbStmtsContext ctx) {
      if (ctx.exception != null) {
        throw ctx.exception;
      }
    }

    @Override
    public void exitCreateModeltype(TrainDBSqlParser.CreateModeltypeContext ctx) {
      String name = ctx.modeltypeName().getText();
      String category = ctx.modeltypeCategory().getText();
      String location = ctx.modeltypeSpecClause().modeltypeLocation().getText();
      if (location.isEmpty()) {
        location = new String("LOCAL");
      }
      String className = ctx.modeltypeSpecClause().modeltypeClassName().getText();
      String uri = ctx.modeltypeSpecClause().modeltypeUri().getText();
      LOG.debug("CREATE MODELTYPE: name=" + name + " category=" + category
          + " location=" + location + " class=" + className + " uri=" + uri);
      commands.add(new TrainDBSqlCreateModeltype(name, category, location, className, uri));
    }

    @Override
    public void exitDropModeltype(TrainDBSqlParser.DropModeltypeContext ctx) {
      String name = ctx.modeltypeName().getText();
      LOG.debug("DROP MODELTYPE: name=" + name);
      commands.add(new TrainDBSqlDropModeltype(name));
    }

    @Override
    public void exitShowStmt(TrainDBSqlParser.ShowStmtContext ctx) {
      String showTarget = ctx.showTargets().getText().toUpperCase();
      Map<String, Object> whereExprMap = getShowWhereExpressionMap(ctx.showWhereClause());
      if (showTarget.equals("MODELTYPES")) {
        commands.add(new TrainDBSqlShowCommand.Modeltypes(whereExprMap));
      } else if (showTarget.equals("MODELS")) {
        commands.add(new TrainDBSqlShowCommand.Models(whereExprMap));
      } else if (showTarget.equals("SYNOPSES")) {
        commands.add(new TrainDBSqlShowCommand.Synopses(whereExprMap));
      } else if (showTarget.equals("SCHEMAS")) {
        commands.add(new TrainDBSqlShowCommand.Schemas(whereExprMap));
      } else if (showTarget.equals("TABLES")) {
        commands.add(new TrainDBSqlShowCommand.Tables(whereExprMap));
      } else if (showTarget.equals("COLUMNS")) {
        commands.add(new TrainDBSqlShowCommand.Columns(whereExprMap));
      } else if (showTarget.equals("HYPERPARAMETERS")) {
        commands.add(new TrainDBSqlShowCommand.Hyperparameters(whereExprMap));
      } else if (showTarget.equals("TRAININGS")) {
        commands.add(new TrainDBSqlShowCommand.Trainings(whereExprMap));
      } else if (showTarget.equals("PARTITIONS")) {
        commands.add(new TrainDBSqlShowCommand.Partitions(whereExprMap));
      } else if (showTarget.equals("QUERYLOGS")) {
        commands.add(new TrainDBSqlShowCommand.QueryLogs(whereExprMap));
      } else if (showTarget.equals("TASKS")) {
        commands.add(new TrainDBSqlShowCommand.Tasks(whereExprMap));
      }
    }

    @Override
    public void exitTrainModel(TrainDBSqlParser.TrainModelContext ctx) {
      String modelName = ctx.modelName().getText();
      String modeltypeName = ctx.modeltypeName().getText();

      List<String> schemaNames = new ArrayList<>();
      List<String> tableNames = new ArrayList<>();
      List<List<String>> columnNames = new ArrayList<>();

      schemaNames.add(ctx.tableName().schemaName().getText());
      tableNames.add(ctx.tableName().tableIdentifier.getText());
      List<String> tableColumnNames = new ArrayList<>();
      for (TrainDBSqlParser.ColumnNameContext columnName : ctx.columnNameList().columnName()) {
        tableColumnNames.add(columnName.getText());
      }
      columnNames.add(tableColumnNames);

      if (ctx.joinTableListOpt() != null) {
        for (TrainDBSqlParser.JoinTableListOptContext jtl : ctx.joinTableListOpt()) {
          schemaNames.add(jtl.tableName().schemaName().getText());
          tableNames.add(jtl.tableName().tableIdentifier.getText());
          List<String> joinTableColumnNames = new ArrayList<>();
          for (TrainDBSqlParser.ColumnNameContext columnName : jtl.columnNameList().columnName()) {
            joinTableColumnNames.add(columnName.getText());
          }
          columnNames.add(joinTableColumnNames);
        }
      }

      String joinCondition = "";
      if (ctx.joinTableConditionListOpt() != null) {
        int start = ctx.joinTableConditionListOpt().tableConditionList().start.getStartIndex();
        int stop = ctx.joinTableConditionListOpt().tableConditionList().stop.getStopIndex();
        joinCondition = ctx.start.getInputStream().getText(new Interval(start, stop));
      }

      float samplePercent = 100;
      if (ctx.trainSampleClause() != null) {
        samplePercent = Float.parseFloat(ctx.trainSampleClause().samplePercent().getText());
      }

      Map<String, Object> trainOptions = new HashMap<>();
      if (ctx.trainModelOptionsClause() != null) {
        for (TrainDBSqlParser.OptionKeyValueContext optionKeyValue
            : ctx.trainModelOptionsClause().optionKeyValueList().optionKeyValue()) {
          trainOptions.put(optionKeyValue.optionKey().getText(),
              getOptionValueObject(optionKeyValue.optionValue(), false));
        }
      }
      LOG.debug("TRAIN MODEL: name=" + modelName + " type=" + modeltypeName);

      commands.add(new TrainDBSqlTrainModel(
          modeltypeName, modelName, schemaNames, tableNames, columnNames, joinCondition,
          samplePercent, trainOptions));
    }

    @Override
    public void exitDropModel(TrainDBSqlParser.DropModelContext ctx) {
      String modelName = ctx.modelName().getText();
      LOG.debug("DROP MODEL: name=" + modelName);
      commands.add(new TrainDBSqlDropModel(modelName));
    }

    @Override
    public void exitCreateSynopsis(TrainDBSqlParser.CreateSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      TrainDBSqlCommand.SynopsisType synopsisType = TrainDBSqlCommand.SynopsisType.DEFAULT;
      if (ctx.synopsisTypeClause() != null) {
        if (ctx.synopsisTypeClause().K_TABLE() != null) {
          synopsisType = TrainDBSqlCommand.SynopsisType.TABLE;
        } else if (ctx.synopsisTypeClause().K_FILE() != null) {
          synopsisType = TrainDBSqlCommand.SynopsisType.FILE;
        }
      }
      String modelName = ctx.modelName().getText();
      if (ctx.limitSizeClause().limitRows() != null) {
        int limitRows = Integer.parseInt(ctx.limitSizeClause().limitRows().getText());
        LOG.debug("CREATE SYNOPSIS: synopsis=" + synopsisName + " model=" + modelName
            + " limitRows=" + limitRows);
        commands.add(
            new TrainDBSqlCreateSynopsis(synopsisName, synopsisType, modelName, limitRows, 0));
      } else if (ctx.limitSizeClause().limitPercent() != null) {
        float limitPercent = Float.parseFloat(ctx.limitSizeClause().limitPercent().getText());
        LOG.debug("CREATE SYNOPSIS: synopsis=" + synopsisName + " model=" + modelName
            + " limitPercent=" + limitPercent);
        commands.add(
            new TrainDBSqlCreateSynopsis(synopsisName, synopsisType, modelName, 0, limitPercent));
      }
    }

    @Override
    public void exitDropSynopsis(TrainDBSqlParser.DropSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      LOG.debug("DROP SYNOPSIS: name=" + synopsisName);
      commands.add(new TrainDBSqlDropSynopsis(synopsisName));
    }

    @Override
    public void exitDeleteQueryLogs(TrainDBSqlParser.DeleteQueryLogsContext ctx) {
      int limitNumber = Integer.parseInt(ctx.limitNumber().getText());
      commands.add(new TrainDBSqlDeleteQueryLogs(limitNumber));
    }

    @Override
    public void exitDeleteTasks(TrainDBSqlParser.DeleteTasksContext ctx) {
      int limitNumber = Integer.parseInt(ctx.limitNumber().getText());
      commands.add(new TrainDBSqlDeleteTasks(limitNumber));
    }

    @Override
    public void exitUseSchema(TrainDBSqlParser.UseSchemaContext ctx) {
      String schemaName = ctx.schemaName().getText();
      commands.add(new TrainDBSqlUseSchema(schemaName));
    }

    @Override
    public void exitDescribeTable(TrainDBSqlParser.DescribeTableContext ctx) {
      String schemaName = null;
      if (ctx.tableName().schemaName() != null) {
        schemaName = ctx.tableName().schemaName().getText();
      }
      String tableName = ctx.tableName().tableIdentifier.getText();
      commands.add(new TrainDBSqlDescribeTable(schemaName, tableName));
    }

    @Override
    public void exitExportModel(TrainDBSqlParser.ExportModelContext ctx) {
      String modelName = ctx.modelName().getText();
      String exportFilename = null;
      if (ctx.exportToClause() != null) {
        exportFilename = ctx.exportToClause().filenameString().getText();
      }
      LOG.debug("EXPORT MODEL: name=" + modelName);
      commands.add(new TrainDBSqlExportModel(modelName, exportFilename));
    }

    @Override
    public void exitImportModel(TrainDBSqlParser.ImportModelContext ctx) {
      String modelName = ctx.modelName().getText();
      String modelBinaryString = null;
      String importFilename = null;
      if (ctx.modelBinaryString() != null) {
        modelBinaryString = ctx.modelBinaryString().getText();
      } else if (ctx.filenameString() != null) {
        importFilename = ctx.filenameString().getText();
      }
      LOG.debug("IMPORT MODEL: name=" + modelName);
      commands.add(new TrainDBSqlImportModel(modelName, modelBinaryString, importFilename));
    }

    @Override
    public void exitAlterModel(TrainDBSqlParser.AlterModelContext ctx) {
      String modelName = ctx.modelName().getText();
      if (ctx.alterModelClause().newModelName() != null) {
        String newModelName = ctx.alterModelClause().newModelName().getText();
        commands.add(new TrainDBSqlAlterModel.Rename(modelName, newModelName));
      }
      if (ctx.alterModelClause().enableDisableClause() != null) {
        String enableOption = ctx.alterModelClause().enableDisableClause().getText();
        if (enableOption.equalsIgnoreCase("ENABLE")) {
          commands.add(new TrainDBSqlAlterModel.Enable(modelName));
        } else if (enableOption.equalsIgnoreCase("DISABLE")) {
          commands.add(new TrainDBSqlAlterModel.Disable(modelName));
        }
      }
      LOG.debug("ALTER MODEL: name=" + modelName);
    }

    @Override
    public void exitExportSynopsis(TrainDBSqlParser.ExportSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      String exportFilename = null;
      if (ctx.exportToClause() != null) {
        exportFilename = ctx.exportToClause().filenameString().getText();
      }
      LOG.debug("EXPORT SYNOPSIS: name=" + synopsisName);
      commands.add(new TrainDBSqlExportSynopsis(synopsisName, exportFilename));
    }

    @Override
    public void exitImportSynopsis(TrainDBSqlParser.ImportSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      TrainDBSqlCommand.SynopsisType synopsisType = TrainDBSqlCommand.SynopsisType.DEFAULT;
      if (ctx.synopsisTypeClause() != null) {
        if (ctx.synopsisTypeClause().K_TABLE() != null) {
          synopsisType = TrainDBSqlCommand.SynopsisType.TABLE;
        } else if (ctx.synopsisTypeClause().K_FILE() != null) {
          synopsisType = TrainDBSqlCommand.SynopsisType.FILE;
        }
      }
      String synopsisBinaryString = null;
      String importFilename = null;
      if (ctx.synopsisBinaryString() != null) {
        synopsisBinaryString = ctx.synopsisBinaryString().getText();
      } else if (ctx.filenameString() != null) {
        importFilename = ctx.filenameString().getText();
      }
      LOG.debug("IMPORT SYNOPSIS: name=" + synopsisName);
      commands.add(new TrainDBSqlImportSynopsis(synopsisName, synopsisType, synopsisBinaryString,
          importFilename));
    }

    @Override
    public void exitAlterSynopsis(TrainDBSqlParser.AlterSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      if (ctx.alterSynopsisClause().newSynopsisName() != null) {
        String newSynopsisName = ctx.alterSynopsisClause().newSynopsisName().getText();
        commands.add(new TrainDBSqlAlterSynopsis.Rename(synopsisName, newSynopsisName));
      }
      if (ctx.alterSynopsisClause().enableDisableClause() != null) {
        String enableOption = ctx.alterSynopsisClause().enableDisableClause().getText();
        if (enableOption.equalsIgnoreCase("ENABLE")) {
          commands.add(new TrainDBSqlAlterSynopsis.Enable(synopsisName));
        } else if (enableOption.equalsIgnoreCase("DISABLE")) {
          commands.add(new TrainDBSqlAlterSynopsis.Disable(synopsisName));
        }
      }
      LOG.debug("ALTER SYNOPSIS: name=" + synopsisName);
    }

    @Override
    public void exitBypassDdlStmt(TrainDBSqlParser.BypassDdlStmtContext ctx) {
      int start = ctx.ddlString().getStart().getStartIndex();
      int stop = ctx.getStop().getStopIndex();
      String stmt = ctx.getStart().getInputStream().getText(new Interval(start, stop));
      LOG.debug("BYPASS DDL: stmt=" + stmt);
      commands.add(new TrainDBSqlBypassDdlStmt(stmt));
    }

    @Override
    public void exitAnalyzeSynopsis(TrainDBSqlParser.AnalyzeSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      LOG.debug("ANALYZE SYNOPSIS: name=" + synopsisName);
      commands.add(new TrainDBSqlAnalyzeSynopsis(synopsisName));
    }

    @Override
    public void exitIncrementalQuery(TrainDBSqlParser.IncrementalQueryContext ctx) {
      int start = ctx.ddlString().getStart().getStartIndex();
      int stop = ctx.getStop().getStopIndex();
      String stmt = ctx.getStart().getInputStream().getText(new Interval(start, stop));
      LOG.debug("INCREMENTAL : stmt=" + stmt);
      commands.add(new TrainDBIncrementalQuery(stmt));
    }

    @Override
    public void exitIncrementalParallelQuery(TrainDBSqlParser.IncrementalParallelQueryContext ctx) {
      int start = ctx.ddlString().getStart().getStartIndex();
      int stop = ctx.getStop().getStopIndex();
      String stmt = ctx.getStart().getInputStream().getText(new Interval(start, stop));
      LOG.debug("INCREMENTAL PARALLEL : stmt=" + stmt);
      commands.add(new TrainDBIncrementalParallelQuery(stmt));
    }
      

    private Object getOptionValueObject(TrainDBSqlParser.OptionValueContext ctx,
                                        boolean convertPattern) {
      if (ctx.STRING_LITERAL() == null) {
        if (ctx.getText().contains(".")) {
          return Double.valueOf(ctx.getText());
        } else {
          return Integer.valueOf(ctx.getText());
        }
      }
      if (convertPattern) {
        return convertPattern(ctx.getText());
      }
      return ctx.getText();
    }

    /**
     * Convert a pattern containing JDBC catalog search wildcards into
     * Java regex patterns.
     *
     * @param pattern input which may contain '%' or '_' wildcard characters
     * @return replace %/_ with regex search characters, also handle escaped characters.
     */
    private String convertPattern(final String pattern) {
      final char searchStringEscape = '\\';

      String convertedPattern;
      if (pattern == null) {
        convertedPattern = ".*";
      } else {
        StringBuilder result = new StringBuilder(pattern.length());

        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
          char c = pattern.charAt(i);
          if (escaped) {
            if (c != searchStringEscape) {
              escaped = false;
            }
            result.append(c);
          } else {
            if (c == searchStringEscape) {
              escaped = true;
            } else if (c == '%') {
              result.append(".*");
            } else if (c == '_') {
              result.append('.');
            } else {
              result.append(c);
            }
          }
        }

        convertedPattern = result.toString();
      }

      return convertedPattern;
    }

    private Map<String, Object> getShowWhereExpressionMap(
        TrainDBSqlParser.ShowWhereClauseContext ctx) {
      Map<String, Object> showWhereExpressionMap = new HashMap<>();
      if (ctx != null) {
        for (TrainDBSqlParser.ShowWhereExpressionContext showWhereExprCtx
            : ctx.showWhereExpressionList().showWhereExpression()) {
          String op = showWhereExprCtx.showFilterOperator().getText();
          showWhereExpressionMap.put(showWhereExprCtx.showFilterKey().getText(),
              getOptionValueObject(showWhereExprCtx.optionValue(), op.equalsIgnoreCase("LIKE")));
        }
      }
      return showWhereExpressionMap;
    }

    List<TrainDBSqlCommand> getSqlCommands() {
      return commands;
    }
  }
}
