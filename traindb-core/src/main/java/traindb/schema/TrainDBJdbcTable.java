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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import traindb.common.TrainDBLogger;

public final class TrainDBJdbcTable extends AbstractQueryableTable
    implements TranslatableTable, ScannableTable {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBJdbcTable.class);
  private final String name;
  private final TrainDBJdbcSchema jdbcSchema;
  private Schema.TableType tableType;
  private RelProtoDataType protoRowType;

  public TrainDBJdbcTable(String name, TrainDBJdbcSchema schema, MetaImpl.MetaTable tableDef,
                          DatabaseMetaData databaseMetaData) {
    super(Object[].class);
    this.name = name;
    this.tableType = Schema.TableType.valueOf(tableDef.tableType);
    this.jdbcSchema = schema;

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
    protoRowType = RelDataTypeImpl.proto(builder.build());
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

  @Override
  public String toString() {
    return "TrainDBTable {" + getName() + "}";
  }

  @Override
  public final Schema.TableType getJdbcTableType() {
    return tableType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return protoRowType.apply(relDataTypeFactory);
  }

  private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses(final JavaTypeFactory typeFactory) {
    final RelDataType rowType = getRowType(typeFactory);
    return Util.transform(rowType.getFieldList(), f -> {
      final RelDataType type = f.getType();
      final Class clazz = (Class) typeFactory.getJavaClass(type);
      final ColumnMetaData.Rep rep =
          Util.first(ColumnMetaData.Rep.of(clazz),
              ColumnMetaData.Rep.OBJECT);
      return Pair.of(rep, type.getSqlTypeName().getJdbcOrdinal());
    });
  }

  SqlString generateSql() {
    final SqlNodeList selectList = SqlNodeList.SINGLETON_STAR;
    SqlSelect node =
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList,
            tableName(), null, null, null, null, null, null, null, null);
    final SqlWriterConfig config = SqlPrettyWriter.config()
        .withAlwaysUseParentheses(true)
        .withDialect(getJdbcDataSource().getDialect());
    final SqlPrettyWriter writer = new SqlPrettyWriter(config);
    node.unparse(writer, 0, 0);
    return writer.toSqlString();
  }

  SqlIdentifier tableName() {
    final List<String> strings = new ArrayList<>();
    strings.add(getJdbcSchema().getName());
    strings.add(getName());
    return new SqlIdentifier(strings, SqlParserPos.ZERO);
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new JdbcTableScan(context.getCluster(), relOptTable, this,
        getJdbcDataSource().getConvention());
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return new JdbcTableQueryable<>(queryProvider, schema, tableName);
  }

  public Enumerable<Object[]> scan(DataContext root) {
    JavaTypeFactory typeFactory = root.getTypeFactory();
    final SqlString sql = generateSql();
    return ResultSetEnumerable.of(getJdbcDataSource().getDataSource(), sql.getSql(),
        JdbcUtils.rowBuilderFactory2(fieldClasses(typeFactory)));
  }

  public final String getName() {
    return name;
  }

  public final TrainDBJdbcSchema getJdbcSchema() {
    return jdbcSchema;
  }

  public final TrainDBJdbcDataSource getJdbcDataSource() {
    return jdbcSchema.getJdbcDataSource();
  }

  private class JdbcTableQueryable<T> extends AbstractTableQueryable<T> {
    JdbcTableQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
      super(queryProvider, schema, TrainDBJdbcTable.this, tableName);
    }

    @Override
    public String toString() {
      return "JdbcTableQueryable {table: " + tableName + "}";
    }

    @Override
    public Enumerator<T> enumerator() {
      final JavaTypeFactory typeFactory =
          ((CalciteConnection) queryProvider).getTypeFactory();
      final SqlString sql = generateSql();
      final List<Pair<ColumnMetaData.Rep, Integer>> pairs = fieldClasses(typeFactory);
      @SuppressWarnings({"rawtypes", "unchecked"}) final Enumerable<T> enumerable =
          (Enumerable) ResultSetEnumerable.of(getJdbcDataSource().getDataSource(), sql.getSql(),
              JdbcUtils.rowBuilderFactory2(pairs));
      return enumerable.enumerator();
    }
  }
}
