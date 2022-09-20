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
import javax.annotation.Nullable;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
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
        RelDataType sqlType = sqlType(typeFactory, dataType, precision, scale, typeString);

        builder.add(columnName, sqlType);
      }
    } catch (SQLException e) {
      LOG.debug(e.getMessage());
      JdbcUtils.close(null, null, resultSet);
    }

    return builder.build();
  }

  private static RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                     int precision, int scale, @Nullable String typeString) {
    // Fall back to ANY if type is unknown
    final SqlTypeName sqlTypeName =
        Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
    switch (sqlTypeName) {
      case ARRAY:
        RelDataType component = null;
        if (typeString != null && typeString.endsWith(" ARRAY")) {
          // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
          // "INTEGER".
          final String remaining = typeString.substring(0,
              typeString.length() - " ARRAY".length());
          component = parseTypeString(typeFactory, remaining);
        }
        if (component == null) {
          component = typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
        return typeFactory.createArrayType(component, -1);
      default:
        break;
    }
    if (precision >= 0
        && scale >= 0
        && sqlTypeName.allowsPrecScale(true, true)) {
      return typeFactory.createSqlType(sqlTypeName, precision, scale);
    } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
      return typeFactory.createSqlType(sqlTypeName, precision);
    } else {
      assert sqlTypeName.allowsNoPrecNoScale();
      return typeFactory.createSqlType(sqlTypeName);
    }
  }

  /**
   * Given "INTEGER", returns BasicSqlType(INTEGER).
   * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
   * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).
   */
  private static RelDataType parseTypeString(RelDataTypeFactory typeFactory,
                                             String typeString) {
    int precision = -1;
    int scale = -1;
    int open = typeString.indexOf("(");
    if (open >= 0) {
      int close = typeString.indexOf(")", open);
      if (close >= 0) {
        String rest = typeString.substring(open + 1, close);
        typeString = typeString.substring(0, open);
        int comma = rest.indexOf(",");
        if (comma >= 0) {
          precision = Integer.parseInt(rest.substring(0, comma));
          scale = Integer.parseInt(rest.substring(comma));
        } else {
          precision = Integer.parseInt(rest);
        }
      }
    }
    try {
      final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
      return typeName.allowsPrecScale(true, true)
          ? typeFactory.createSqlType(typeName, precision, scale)
          : typeName.allowsPrecScale(true, false)
          ? typeFactory.createSqlType(typeName, precision)
          : typeFactory.createSqlType(typeName);
    } catch (IllegalArgumentException e) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    }
  }
}
