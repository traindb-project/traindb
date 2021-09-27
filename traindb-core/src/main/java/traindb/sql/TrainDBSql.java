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
import traindb.common.TrainDBLogger;

public final class TrainDBSql {

  private TrainDBSql() {
  }

  public static List<TrainDBSqlCommand> parse(String query) {
    ANTLRInputStream input = new ANTLRInputStream(query);
    TrainDBSqlLexer lexer = new TrainDBSqlLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    TrainDBSqlParser parser = new TrainDBSqlParser(tokens);

    // remove default console output printing error listener
    // to suppress syntax error messages for TrainDB (such input query is passed to VerdictDB)
    lexer.removeErrorListener(ConsoleErrorListener.INSTANCE);
    parser.removeErrorListener(ConsoleErrorListener.INSTANCE);

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
            createModel.getModelLocation(), createModel.getModelUri());
        break;
      case DROP_MODEL:
        TrainDBSqlDropModel dropModel = (TrainDBSqlDropModel) command;
        runner.dropModel(dropModel.getModelName());
        break;
      case SHOW_MODELS:
        TrainDBSqlShowCommand showCmd = (TrainDBSqlShowCommand) command;
        return runner.showModels();
      default:
        throw new RuntimeException("invalid TrainDB SQL command");
    }
    return null;
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
      String modelLocation = ctx.modelLocation().getText();
      String modelUri = ctx.modelUri().getText();
      LOG.debug("CREATE MODEL: name=" + modelName + " type=" + modelType +
          " location=" + modelLocation + " uri=" + modelUri);
      commands.add(new TrainDBSqlCreateModel(modelName, modelType, modelLocation, modelUri));
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

    List<TrainDBSqlCommand> getSqlCommands() {
      return commands;
    }
  }
}
