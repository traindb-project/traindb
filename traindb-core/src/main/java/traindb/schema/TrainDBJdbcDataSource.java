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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import traindb.common.TrainDBLogger;

public class TrainDBJdbcDataSource extends AbstractSchema {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBJdbcDataSource.class);

  private final String name;
  private final DataSource dataSource;
  private final SqlDialect dialect;
  private final JdbcConvention convention;
  private ImmutableMap<String, Schema> subSchemaMap;

  public TrainDBJdbcDataSource(SchemaPlus parentSchema, DataSource dataSource) {
    this.name = "traindb"; // FIXME
    this.dataSource = dataSource;
    final Expression expression =
        Schemas.subSchemaExpression(parentSchema, name, JdbcCatalogSchema.class);
    this.dialect = createDialect(dataSource);
    this.convention = JdbcConvention.of(dialect, expression, name);
    computeSubSchemaMap();
  }

  public void computeSubSchemaMap() {
    final ImmutableMap.Builder<String, Schema> builder = ImmutableMap.builder();
    Connection connection = null;
    ResultSet resultSet = null;
    try {
      connection = dataSource.getConnection();
      resultSet = connection.getMetaData().getCatalogs();
      while (resultSet.next()) {
        final String schemaName = requireNonNull(
            resultSet.getString(1),
            () -> "got null schemaName from the database");

        builder.put(schemaName, new TrainDBJdbcSchema(schemaName, this));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(connection, null, resultSet);
    }
    setSubSchemaMap(builder.build());
  }

  /**
   * Returns a suitable SQL dialect for the given data source.
   */
  public static SqlDialect createDialect(DataSource dataSource) {
    return JdbcUtils.DialectPool.INSTANCE.get(dataSource);
  }

  public final String getName() {
    return name;
  }

  @Override
  public final boolean isMutable() {
    return false;
  }

  @Override
  public final Map<String, Schema> getSubSchemaMap() {
    LOG.debug("getSubSchemaMap called. subSchemaMapSize=" + subSchemaMap.size());
    return subSchemaMap;
  }

  public final void setSubSchemaMap(ImmutableMap<String, Schema> subSchemaMap) {
    this.subSchemaMap = subSchemaMap;
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
