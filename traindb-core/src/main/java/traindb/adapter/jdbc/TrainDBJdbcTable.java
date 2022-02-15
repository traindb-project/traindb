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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
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
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import traindb.common.TrainDBLogger;
import traindb.schema.TrainDBTable;

public final class TrainDBJdbcTable extends TrainDBTable
    implements TranslatableTable, ScannableTable {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBJdbcTable.class);

  public TrainDBJdbcTable(String name, TrainDBJdbcSchema schema, MetaImpl.MetaTable tableDef,
                          RelDataType protoType) {
    super(name, schema, Schema.TableType.valueOf(tableDef.tableType), protoType);
  }

  @Override
  public String toString() {
    return "TrainDBTable {" + getName() + "}";
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
        .withDialect(getDataSource().getDialect());
    final SqlPrettyWriter writer = new SqlPrettyWriter(config);
    node.unparse(writer, 0, 0);
    return writer.toSqlString();
  }

  SqlIdentifier tableName() {
    final List<String> strings = new ArrayList<>();
    strings.add(getSchema().getName());
    strings.add(getName());
    return new SqlIdentifier(strings, SqlParserPos.ZERO);
  }

  private final TrainDBJdbcDataSource getDataSource() {
    return (TrainDBJdbcDataSource) getSchema().getDataSource();
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return new JdbcTableScan(context.getCluster(), context.getTableHints(), relOptTable, this,
        getDataSource().getConvention());
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return new JdbcTableQueryable<>(queryProvider, schema, tableName);
  }

  public Enumerable<Object[]> scan(DataContext root) {
    JavaTypeFactory typeFactory = root.getTypeFactory();
    final SqlString sql = generateSql();
    return ResultSetEnumerable.of(getDataSource().getDataSource(), sql.getSql(),
        JdbcUtils.rowBuilderFactory2(fieldClasses(typeFactory)));
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
          (Enumerable) ResultSetEnumerable.of(getDataSource().getDataSource(), sql.getSql(),
              JdbcUtils.rowBuilderFactory2(pairs));
      return enumerable.enumerator();
    }
  }
}
