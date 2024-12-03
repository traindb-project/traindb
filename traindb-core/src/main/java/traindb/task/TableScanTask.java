package traindb.task;

import java.util.concurrent.Callable;
import org.apache.calcite.jdbc.CalcitePrepare.Context;
import traindb.adapter.jdbc.JdbcRel;
import traindb.engine.TrainDBListResultSet;

public class TableScanTask implements Callable<TrainDBListResultSet> {

  Context context;
  JdbcRel target;

  public TableScanTask(Context context, JdbcRel target) {
    this.context = context;
    this.target = target;
  }

  @Override
  public TrainDBListResultSet call() {
    TrainDBListResultSet result = target.execute(context);

    return result;
  }
}