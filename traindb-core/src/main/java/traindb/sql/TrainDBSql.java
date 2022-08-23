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
import traindb.common.TrainDBLogger;
import traindb.engine.TrainDBListResultSet;

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
        return runner.showModeltypes();
      case SHOW_MODELS:
        TrainDBSqlShowCommand showModels = (TrainDBSqlShowCommand) command;
        return runner.showModels();
      case TRAIN_MODEL:
        TrainDBSqlTrainModel trainModel = (TrainDBSqlTrainModel) command;
        runner.trainModel(
            trainModel.getModeltypeName(), trainModel.getModelName(),
            trainModel.getSchemaName(), trainModel.getTableName(),
            trainModel.getColumnNames(), trainModel.getTrainOptions());
        break;
      case DROP_MODEL:
        TrainDBSqlDropModel dropModel = (TrainDBSqlDropModel) command;
        runner.dropModel(dropModel.getModelName());
        break;
      case CREATE_SYNOPSIS:
        TrainDBSqlCreateSynopsis createSynopsis = (TrainDBSqlCreateSynopsis) command;
        runner.createSynopsis(createSynopsis.getSynopsisName(),
            createSynopsis.getModelName(), createSynopsis.getLimitNumber());
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
      case BYPASS_DDL_STMT:
        TrainDBSqlBypassDdlStmt bypassDdlStmt = (TrainDBSqlBypassDdlStmt) command;
        runner.bypassDdlStmt(bypassDdlStmt.getStatement());
        break;
      default:
        throw new RuntimeException("invalid TrainDB SQL command");
    }
    return TrainDBListResultSet.empty();
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
      String className = ctx.modeltypeSpecClause().modeltypeClassName().getText();
      String uri = ctx.modeltypeSpecClause().modeltypeUri().getText();
      LOG.debug("CREATE MODELTYPE: name=" + name + " category=" + category
          + " location=" + location + " class=" + className  + " uri=" + uri);
      commands.add(new TrainDBSqlCreateModeltype(name, category, location, className, uri));
    }

    @Override
    public void exitDropModeltype(TrainDBSqlParser.DropModeltypeContext ctx) {
      String name = ctx.modeltypeName().getText();
      LOG.debug("DROP MODELTYPE: name=" + name);
      commands.add(new TrainDBSqlDropModeltype(name));
    }

    @Override
    public void exitShowModeltypes(TrainDBSqlParser.ShowModeltypesContext ctx) {
      commands.add(new TrainDBSqlShowCommand.Modeltypes());
    }

    @Override
    public void exitShowModels(TrainDBSqlParser.ShowModelsContext ctx) {
      commands.add(new TrainDBSqlShowCommand.Models());
    }

    @Override
    public void exitTrainModel(TrainDBSqlParser.TrainModelContext ctx) {
      String modelName = ctx.modelName().getText();
      String modeltypeName = ctx.modeltypeName().getText();

      List<String> columnNames = new ArrayList<>();
      for (TrainDBSqlParser.ColumnNameContext columnName : ctx.columnNameList().columnName()) {
        columnNames.add(columnName.getText());
      }

      Map<String, Object> trainOptions = new HashMap<>();
      if (ctx.trainModelOptionsClause() != null) {
        for (TrainDBSqlParser.OptionKeyValueContext optionKeyValue
            : ctx.trainModelOptionsClause().optionKeyValueList().optionKeyValue()) {
          Object value;
          if (optionKeyValue.optionValue().STRING_LITERAL() == null) {
            if (optionKeyValue.optionValue().getText().contains(".")) {
              value = Double.valueOf(optionKeyValue.optionValue().getText());
            } else {
              value = Integer.valueOf(optionKeyValue.optionValue().getText());
            }
          } else {
            value = optionKeyValue.optionValue().getText();
          }
          trainOptions.put(optionKeyValue.optionKey().getText(), value);
        }
      }
      LOG.debug("TRAIN MODEL: name=" + modelName + " type=" + modeltypeName);

      String schemaName = ctx.tableName().schemaName().getText();
      String tableName = ctx.tableName().tableIdentifier.getText();
      commands.add(new TrainDBSqlTrainModel(
          modeltypeName, modelName, schemaName, tableName, columnNames, trainOptions));
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
      String modelName = ctx.modelName().getText();
      int limitNumber = Integer.parseInt(ctx.limitNumber().getText());
      LOG.debug("CREATE SYNOPSIS: synopsis=" + synopsisName + " model=" + modelName
          + " limit=" + limitNumber);
      commands.add(new TrainDBSqlCreateSynopsis(synopsisName, modelName, limitNumber));
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
      String schemaName = null;
      if (ctx.tableName().schemaName() != null) {
        schemaName = ctx.tableName().schemaName().getText();
      }
      String tableName = ctx.tableName().tableIdentifier.getText();
      commands.add(new TrainDBSqlDescribeTable(schemaName, tableName));
    }

    @Override
    public void exitBypassDdlStmt(TrainDBSqlParser.BypassDdlStmtContext ctx) {
      int start = ctx.ddlString().getStart().getStartIndex();
      int stop = ctx.getStop().getStopIndex();
      String stmt = ctx.getStart().getInputStream().getText(new Interval(start, stop));
      LOG.debug("BYPASS DDL: stmt=" + stmt);
      commands.add(new TrainDBSqlBypassDdlStmt(stmt));
    }

    List<TrainDBSqlCommand> getSqlCommands() {
      return commands;
    }
  }
}
