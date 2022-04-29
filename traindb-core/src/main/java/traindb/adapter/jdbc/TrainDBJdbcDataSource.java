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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlDialect;
import traindb.adapter.TrainDBSqlDialect;
import traindb.common.TrainDBLogger;
import traindb.schema.TrainDBDataSource;

public class TrainDBJdbcDataSource extends TrainDBDataSource {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBJdbcDataSource.class);

  private final DataSource dataSource;
  private final SqlDialect dialect;
  private final JdbcConvention convention;

  public TrainDBJdbcDataSource(SchemaPlus parentSchema, DataSource dataSource) {
    super();
    this.dataSource = dataSource;
    final Expression expression =
        Schemas.subSchemaExpression(parentSchema, getName(), TrainDBJdbcSchema.class);
    this.dialect = createDialect(dataSource);
    this.convention = JdbcConvention.of(dialect, expression, getName());
    computeSubSchemaMap();
  }

  public void computeSubSchemaMap() {
    final ImmutableMap.Builder<String, Schema> builder = ImmutableMap.builder();
    Connection connection = null;
    ResultSet resultSet = null;
    try {
      connection = dataSource.getConnection();
      if (dialect instanceof TrainDBSqlDialect &&
          !((TrainDBSqlDialect) dialect).supportCatalogs()) {
        String schemaName = connection.getMetaData().getUserName();
        builder.put(schemaName, new TrainDBJdbcSchema(schemaName, this));
      } else {
        resultSet = connection.getMetaData().getCatalogs();
        while (resultSet.next()) {
          final String schemaName = requireNonNull(
              resultSet.getString(1),
              () -> "got null schemaName from the database");
          builder.put(schemaName, new TrainDBJdbcSchema(schemaName, this));
        }
      }
      setSubSchemaMap(builder.build());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(connection, null, resultSet);
    }
  }

  /**
   * Returns a suitable SQL dialect for the given data source.
   */
  public static SqlDialect createDialect(DataSource dataSource) {
    return JdbcUtils.DialectPool.INSTANCE.get(dataSource);
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public SqlDialect getDialect() {
    return dialect;
  }

  public JdbcConvention getConvention() {
    return convention;
  }
}
