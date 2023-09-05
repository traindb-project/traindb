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

package traindb.adapter.jdbc;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import traindb.adapter.TrainDBSqlDialect;
import traindb.common.TrainDBLogger;
import traindb.schema.TrainDBSchema;

public class TrainDBJdbcSchema extends TrainDBSchema {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBJdbcSchema.class);

  public TrainDBJdbcSchema(String name, TrainDBJdbcDataSource dataSource) {
    super(name, dataSource);
    computeTableMap();
  }

  public void computeTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    Connection connection = null;
    ResultSet resultSet = null;

    try {
      TrainDBJdbcDataSource dataSource = (TrainDBJdbcDataSource) getDataSource();
      connection = dataSource.getDataSource().getConnection();
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      SqlDialect dialect = ((TrainDBJdbcDataSource) getDataSource()).getDialect();
      if (!(dialect instanceof TrainDBSqlDialect)
          || ((TrainDBSqlDialect) dialect).supportCatalogs()) {
        resultSet = databaseMetaData.getTables(getName(), null, null, null);
      } else {
        resultSet = databaseMetaData.getTables(null, getName(), null, null);
      }
      while (resultSet.next()) {
        final String catalogName = resultSet.getString(1);
        final String schemaName = resultSet.getString(2);
        final String tableName = resultSet.getString(3);

        // DB users can access the tables of implicit schemas - ISSUE #41
        if (!schemaName.equals(getName())) {
          continue;
        }

        // original code
        //final String tableTypeName = resultSet.getString(4).replace(" ", "_");

        // -----> for postgres code start
        String tableTypeName = resultSet.getString(4);

        if ( tableTypeName == null || tableTypeName.length() == 0 )
          tableTypeName = "TABLE";
        else
          tableTypeName = resultSet.getString(4).replace(" ", "_");
        // -----> for postgres code end

        MetaImpl.MetaTable tableDef =
            new MetaImpl.MetaTable(catalogName, schemaName, tableName, tableTypeName);

        builder.put(tableName, new TrainDBJdbcTable(tableName, this, tableDef,
                                                    getProtoType(tableDef, databaseMetaData)));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(connection, null, resultSet);
    }
    setTableMap(builder.build());
  }

  private RelDataType getProtoType(MetaImpl.MetaTable tableDef, DatabaseMetaData databaseMetaData) {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    ResultSet resultSet = null;
    try {
      resultSet = databaseMetaData.getColumns(
          tableDef.tableCat, tableDef.tableSchem, tableDef.tableName, null);
      while (resultSet.next()) {
        String columnName = resultSet.getString(4);
        int dataType = resultSet.getInt(5);
        String typeString = resultSet.getString(6);
        boolean nullable = (resultSet.getInt(11) != databaseMetaData.columnNoNulls);
        int precision;
        int scale;
        switch (SqlType.valueOf(dataType)) {
          case TIMESTAMP:
          case TIME:
            precision = resultSet.getInt(9); // SCALE
            scale = 0;
            break;
          default:
            precision = resultSet.getInt(7); // SIZE
            scale = resultSet.getInt(9); // SCALE
            break;
        }
        RelDataType sqlType =
            sqlType(typeFactory, dataType, precision, scale, nullable, typeString);

        builder.add(columnName, sqlType);
      }
    } catch (SQLException e) {
      LOG.debug(e.getMessage());
      JdbcUtils.close(null, null, resultSet);
    }

    return builder.build();
  }

}
