/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package traindb.adapter.jdbc;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;

import com.google.common.collect.ImmutableList;

import traindb.engine.TrainDBListResultSet;
import traindb.jdbc.TrainDBConnectionImpl;
import traindb.schema.SchemaManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class JdbcTableScan extends TableScan implements JdbcRel {
  public final TrainDBJdbcTable jdbcTable;

  public JdbcTableScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      TrainDBJdbcTable jdbcTable,
      JdbcConvention jdbcConvention) {
    super(cluster, cluster.traitSetOf(jdbcConvention), hints, table);
    this.jdbcTable = Objects.requireNonNull(jdbcTable, "jdbcTable");
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new JdbcTableScan(
        getCluster(), getHints(), table, jdbcTable, (JdbcConvention) castNonNull(getConvention()));
  }

  @Override public JdbcImplementor.Result implement(
      JdbcImplementor implementor) {
    return implementor.result(jdbcTable.tableName(),
        ImmutableList.of(JdbcImplementor.Clause.FROM), this, null);
  }

  public TrainDBListResultSet execute(org.apache.calcite.jdbc.CalcitePrepare.Context context, String sql) {
    TrainDBListResultSet res = null;
    try {
      TrainDBConnectionImpl conn = (TrainDBConnectionImpl) context.getDataContext().getQueryProvider();

      List<List<Object>> totalRes = new ArrayList<>();
      List<String> header = new ArrayList<>();

      /*
      final JdbcConvention jdbcConvention = (JdbcConvention) requireNonNull(getConvention(),
        () -> "child.getConvention() is null for " + this);
      SqlDialect dialect = jdbcConvention.dialect;
      final JdbcImplementor jdbcImplementor = new JdbcImplementor(dialect,
          (JavaTypeFactory) getCluster().getTypeFactory());
      final SqlImplementor.Result result = jdbcImplementor.visitInput(this, 0);
      String query = result.asStatement().toSqlString(dialect).getSql();
      */
      
      SchemaManager schemaManager = conn.getSchemaManager();
      Connection extConn = conn.getDataSourceConnection();
      Statement stmt = extConn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);

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

      for (int j = 1; j <= columnCount; j++) {
        header.add(md.getColumnName(j));
      }

      res = new TrainDBListResultSet(header, totalRes);

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return res;
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    Convention convention = requireNonNull(getConvention(), "getConvention()");
    return new JdbcTableScan(getCluster(), hintList, getTable(), jdbcTable,
        (JdbcConvention) convention);
  }
}