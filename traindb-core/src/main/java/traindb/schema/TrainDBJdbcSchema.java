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

package traindb.schema;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import traindb.common.TrainDBLogger;

public class TrainDBJdbcSchema extends AbstractSchema {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBJdbcSchema.class);
  private final String name;
  private TrainDBJdbcDataSource jdbcDataSource;
  private ImmutableMap<String, Table> tableMap;

  public TrainDBJdbcSchema(String name, TrainDBJdbcDataSource jdbcDataSource) {
    this.name = name;
    this.jdbcDataSource = jdbcDataSource;
    computeTableMap();
  }

  public void computeTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    Connection connection = null;
    ResultSet resultSet = null;
    try {
      connection = jdbcDataSource.getDataSource().getConnection();
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      resultSet = databaseMetaData.getTables(name, null, null, null);
      while (resultSet.next()) {
        final String catalogName = resultSet.getString(1);
        final String schemaName = resultSet.getString(2);
        final String tableName = resultSet.getString(3);
        final String tableTypeName = resultSet.getString(4).replace(" ", "_");

        builder.put(tableName, new TrainDBJdbcTable(tableName, this,
            new MetaImpl.MetaTable(catalogName, schemaName, tableName, tableTypeName),
            databaseMetaData));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(connection, null, resultSet);
    }
    setTableMap(builder.build());
  }

  public final String getName() {
    return name;
  }

  @Override
  public final boolean isMutable() {
    return false;
  }

  @Override
  public final Map<String, Table> getTableMap() {
    LOG.debug("getTableMap called. tableMapSize: " + tableMap.size());
    if (tableMap == null) {
      computeTableMap();
    }
    return tableMap;
  }

  public final void setTableMap(ImmutableMap<String, Table> tableMap) {
    this.tableMap = tableMap;
  }

  public final TrainDBJdbcDataSource getJdbcDataSource() {
    return jdbcDataSource;
  }
}
