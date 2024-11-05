package traindb.task;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.calcite.jdbc.CalcitePrepare.Context;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import traindb.adapter.jdbc.JdbcUtils;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.SchemaManager;
import traindb.sql.TrainDBIncrementalQuery;
import traindb.sql.TrainDBSqlCommand;

public class IncrementalScanTask implements Callable<List<List<Object>>> {

  Context context;
  TrainDBSqlCommand commands;
  int queryIdx;

  public IncrementalScanTask(Context context, TrainDBSqlCommand commands, int queryIdx) {
    this.context = context;
    this.commands = commands;
    this.queryIdx = queryIdx;
  }

  @Override
  public List<List<Object>> call() {
    TrainDBConnectionImpl conn = 
                (TrainDBConnectionImpl) context.getDataContext().getQueryProvider();

    TaskCoordinator taskCoordinator = conn.getTaskCoordinator();

    TrainDBIncrementalQuery incrementalQuery = (TrainDBIncrementalQuery) commands;
    String sql = incrementalQuery.getStatement();

    List<List<Object>> totalRes = new ArrayList<>();
    List<String> header = new ArrayList<>();

    if (queryIdx <= 0) {
      throw new RuntimeException(
          "failed to run statement: " + sql
              + "\nerror msg: incremental query can be executed on partitioned table only.");
    }

    if (taskCoordinator.saveQuery.size() <= queryIdx) {
      return null;
    }

    try {
      String currentIncrementalQuery = taskCoordinator.saveQuery.get(queryIdx);
      Connection extConn = conn.getDataSourceConnection();
      Statement stmt = extConn.createStatement();
      ResultSet rs = stmt.executeQuery(currentIncrementalQuery);

      int columnCount = rs.getMetaData().getColumnCount();
      ResultSetMetaData md = rs.getMetaData();

      while (rs.next()) {
        List<Object> r = new ArrayList<>();
        for (int j = 1; j <= columnCount; j++) {
          int type = md.getColumnType(j);
          SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(type);
          if (sqlTypeName == DECIMAL) {
            r.add(rs.getInt(j));
          } else {
            r.add(rs.getObject(j));
          }
        }
        totalRes.add(r);
      }

      for (int j = 0; j < taskCoordinator.aggCalls.size(); j++) {
        SqlAggFunction agg = taskCoordinator.aggCalls.get(j);
        header.add(agg.getName());
      }
      JdbcUtils.close(extConn, stmt, rs);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return totalRes;
  }
}