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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import traindb.sql.calcite.GeometryObjectSqlType;

public abstract class TrainDBSchema extends AbstractSchema {
  private final String name;
  private TrainDBDataSource dataSource;
  private ImmutableMap<String, Table> tableMap;
  private ImmutableMap<String, TrainDBPartition> partitionMap;

  public TrainDBSchema(String name, TrainDBDataSource dataSource) {
    this.name = name;
    this.dataSource = dataSource;
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
    return tableMap;
  }

  public final void setTableMap(ImmutableMap<String, Table> tableMap) {
    this.tableMap = tableMap;
  }

  public final Map<String, TrainDBPartition> getPartitionMap() {
    return partitionMap;
  }

  public final void setPartitionMap(ImmutableMap<String, TrainDBPartition> partitionMap) {
    this.partitionMap = partitionMap;
  }

  public final TrainDBDataSource getDataSource() {
    return dataSource;
  }

  protected static RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                       int precision, int scale, boolean nullable,
                                       @Nullable String typeString) {
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
      case ANY:
      case BINARY:
        List<String> geomTypes = ImmutableList.of("GEOMETRY", "POINT", "LINESTRING", "POLYGON",
            "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION");
        if (typeString.startsWith("ST_") || geomTypes.contains(typeString)) {
          RelDataType geomDataType = typeFactory.createSqlType(SqlTypeName.GEOMETRY);
          return new GeometryObjectSqlType(typeString, nullable, geomDataType.getComparability());
        }
        break;
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
  protected static RelDataType parseTypeString(RelDataTypeFactory typeFactory, String typeString) {
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
