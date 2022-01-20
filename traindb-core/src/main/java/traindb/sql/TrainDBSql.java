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
import java.util.List;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import traindb.common.TrainDBLogger;

public final class TrainDBSql {

  private TrainDBSql() {
  }

  public static List<TrainDBSqlCommand> parse(String query) {
    ANTLRInputStream input = new ANTLRInputStream(query);
    TrainDBSqlLexer lexer = new TrainDBSqlLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    TrainDBSqlParser parser = new TrainDBSqlParser(tokens);
    TrainDBErrorListener trainDBErrorListener = new TrainDBErrorListener();

    // remove default console output printing error listener
    // to suppress syntax error messages for TrainDB (such input query is passed to VerdictDB)
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

  public static VerdictSingleResult run(TrainDBSqlCommand command, TrainDBSqlRunner runner)
      throws Exception {
    switch (command.getType()) {
      case CREATE_MODEL:
        TrainDBSqlCreateModel createModel = (TrainDBSqlCreateModel) command;
        runner.createModel(createModel.getModelName(), createModel.getModelType(),
            createModel.getModelLocation(), createModel.getModelClassName(),
            createModel.getModelUri());
        break;
      case DROP_MODEL:
        TrainDBSqlDropModel dropModel = (TrainDBSqlDropModel) command;
        runner.dropModel(dropModel.getModelName());
        break;
      case SHOW_MODELS:
        TrainDBSqlShowCommand showModels = (TrainDBSqlShowCommand) command;
        return runner.showModels();
      case SHOW_MODEL_INSTANCES:
        TrainDBSqlShowCommand showModelInstances = (TrainDBSqlShowCommand) command;
        return runner.showModelInstances();
      case TRAIN_MODEL_INSTANCE:
        TrainDBSqlTrainModelInstance trainModelInstance = (TrainDBSqlTrainModelInstance) command;
        runner.trainModelInstance(
            trainModelInstance.getModelName(), trainModelInstance.getModelInstanceName(),
            trainModelInstance.getSchemaName(), trainModelInstance.getTableName(),
            trainModelInstance.getColumnNames());
        break;
      case DROP_MODEL_INSTANCE:
        TrainDBSqlDropModelInstance dropModelInstance = (TrainDBSqlDropModelInstance) command;
        runner.dropModelInstance(dropModelInstance.getModelInstanceName());
        break;
      case CREATE_SYNOPSIS:
        TrainDBSqlCreateSynopsis createSynopsis = (TrainDBSqlCreateSynopsis) command;
        runner.createSynopsis(createSynopsis.getSynopsisName(),
            createSynopsis.getModelInstanceName(), createSynopsis.getLimitNumber());
        break;
      case DROP_SYNOPSIS:
        TrainDBSqlDropSynopsis dropSynopsis = (TrainDBSqlDropSynopsis) command;
        runner.dropSynopsis(dropSynopsis.getSynopsisName());
        break;
      case SHOW_SYNOPSES:
        TrainDBSqlShowCommand showSynopses = (TrainDBSqlShowCommand) command;
        return runner.showSynopses();
      case SHOW_SCHEMAS:
        TrainDBSqlShowCommand showSchemas = (TrainDBSqlShowCommand) command;
        return runner.showSchemas();
      case SHOW_TABLES:
        TrainDBSqlShowCommand showTables = (TrainDBSqlShowCommand) command;
        return runner.showTables();
      case USE_SCHEMA:
        TrainDBSqlUseSchema useSchema = (TrainDBSqlUseSchema) command;
        runner.useSchema(useSchema.getSchemaName());
        break;
      case DESCRIBE_TABLE:
        TrainDBSqlDescribeTable describeTable = (TrainDBSqlDescribeTable) command;
        return runner.describeTable(describeTable.getSchemaName(), describeTable.getTableName());
      default:
        throw new RuntimeException("invalid TrainDB SQL command");
    }
    return VerdictSingleResultFromDbmsQueryResult.empty();
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
    public void exitCreateModel(TrainDBSqlParser.CreateModelContext ctx) {
      String modelName = ctx.modelName().getText();
      String modelType = ctx.modelType().getText();
      String modelLocation = ctx.modelSpecClause().modelLocation().getText();
      String modelClassName = ctx.modelSpecClause().modelClassName().getText();
      String modelUri = ctx.modelSpecClause().modelUri().getText();
      LOG.debug("CREATE MODEL: name=" + modelName + " type=" + modelType
          + " location=" + modelLocation + " class=" + modelClassName + " uri=" + modelUri);
      commands.add(new TrainDBSqlCreateModel(
          modelName, modelType, modelLocation, modelClassName, modelUri));
    }

    @Override
    public void exitDropModel(TrainDBSqlParser.DropModelContext ctx) {
      String modelName = ctx.modelName().getText();
      LOG.debug("DROP MODEL: name=" + modelName);
      commands.add(new TrainDBSqlDropModel(modelName));
    }

    @Override
    public void exitShowModels(TrainDBSqlParser.ShowModelsContext ctx) {
      commands.add(new TrainDBSqlShowCommand.Models());
    }

    @Override
    public void exitShowModelInstances(TrainDBSqlParser.ShowModelInstancesContext ctx) {
      commands.add(new TrainDBSqlShowCommand.ModelInstances());
    }

    @Override
    public void exitTrainModelInstance(TrainDBSqlParser.TrainModelInstanceContext ctx) {
      String modelName = ctx.modelName().getText();
      String modelInstanceName = ctx.modelInstanceName().getText();
      String schemaName = ctx.tableName().schemaName().getText();
      String tableName = ctx.tableName().tableIdentifier.getText();

      List<String> columnNames = new ArrayList<>();
      for (TrainDBSqlParser.ColumnNameContext columnName : ctx.columnNameList().columnName()) {
        columnNames.add(columnName.getText());
      }

      commands.add(new TrainDBSqlTrainModelInstance(
          modelName, modelInstanceName, schemaName, tableName, columnNames));
    }

    @Override
    public void exitDropModelInstance(TrainDBSqlParser.DropModelInstanceContext ctx) {
      String modelInstanceName = ctx.modelInstanceName().getText();
      LOG.debug("DROP MODEL INSTANCE: name=" + modelInstanceName);
      commands.add(new TrainDBSqlDropModelInstance(modelInstanceName));
    }

    @Override
    public void exitCreateSynopsis(TrainDBSqlParser.CreateSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      String modelInstanceName = ctx.modelInstanceName().getText();
      int limitNumber = Integer.parseInt(ctx.limitNumber().getText());
      LOG.debug("CREATE SYNOPSIS: synopsis=" + synopsisName + " instance=" + modelInstanceName
          + " limit=" + limitNumber);
      commands.add(new TrainDBSqlCreateSynopsis(synopsisName, modelInstanceName, limitNumber));
    }

    @Override
    public void exitDropSynopsis(TrainDBSqlParser.DropSynopsisContext ctx) {
      String synopsisName = ctx.synopsisName().getText();
      LOG.debug("DROP SYNOPSIS: name=" + synopsisName);
      commands.add(new TrainDBSqlDropSynopsis(synopsisName));
    }

    @Override
    public void exitShowSynopses(TrainDBSqlParser.ShowSynopsesContext ctx) {
      commands.add(new TrainDBSqlShowCommand.Synopses());
    }

    @Override
    public void exitShowSchemas(TrainDBSqlParser.ShowSchemasContext ctx) {
      commands.add(new TrainDBSqlShowCommand.Schemas());
    }

    @Override
    public void exitShowTables(TrainDBSqlParser.ShowTablesContext ctx) {
      commands.add(new TrainDBSqlShowCommand.Tables());
    }

    @Override
    public void exitUseSchema(TrainDBSqlParser.UseSchemaContext ctx) {
      String schemaName = ctx.schemaName().getText();
      commands.add(new TrainDBSqlUseSchema(schemaName));
    }

    @Override
    public void exitDescribeTable(TrainDBSqlParser.DescribeTableContext ctx) {
      String schemaName = ctx.tableName().schemaName().getText();
      String tableName = ctx.tableName().tableIdentifier.getText();
      commands.add(new TrainDBSqlDescribeTable(schemaName, tableName));
    }

    List<TrainDBSqlCommand> getSqlCommands() {
      return commands;
    }
  }
}
