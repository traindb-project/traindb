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
import javax.annotation.Nullable;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import traindb.catalog.pm.MColumn;
import traindb.catalog.pm.MSchema;
import traindb.catalog.pm.MTable;
import traindb.common.TrainDBLogger;

public class TrainDBCatalogSchema extends TrainDBSchema {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBCatalogSchema.class);

  public TrainDBCatalogSchema(MSchema mSchema, TrainDBCatalogDataSource dataSource) {
    super(mSchema.getSchemaName(), dataSource);
    computeTableMap(mSchema);
  }

  public void computeTableMap(MSchema mSchema) {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    final String catalogName = getDataSource().getName();
    for (MTable mTable : mSchema.getTables()) {
      final String schemaName = mSchema.getSchemaName();
      final String tableName = mTable.getTableName();
      final String tableTypeName = mTable.getTableType();

      MetaImpl.MetaTable tableDef =
          new MetaImpl.MetaTable(catalogName, schemaName, tableName, tableTypeName);

      builder.put(tableName,
          new TrainDBCatalogTable(tableName, this, tableDef, getProtoType(mTable)));
    }
    setTableMap(builder.build());
  }

  private RelDataType getProtoType(MTable mTable) {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (MColumn mColumn : mTable.getColumns()) {
      RelDataType sqlType =
          sqlType(typeFactory, mColumn.getColumnType(),
              mColumn.getPrecision(), mColumn.getScale(), mColumn.isNullable(),
              SqlTypeName.getNameForJdbcType(mColumn.getColumnType()).getName());

      builder.add(mColumn.getColumnName(), sqlType);
    }
    return builder.build();
  }

}
