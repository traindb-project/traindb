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

package traindb.jdbc;

import static org.apache.calcite.avatica.MetaImpl.MetaColumn;
import static org.apache.calcite.avatica.MetaImpl.MetaTable;

import com.google.common.collect.ImmutableMap;
import java.sql.SQLException;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Schema that contains metadata tables such as "TABLES" and "COLUMNS".
 */
class MetadataSchema extends AbstractSchema {
  public static final Schema INSTANCE = new MetadataSchema();
  private static final Map<String, Table> TABLE_MAP =
      ImmutableMap.of(
          "COLUMNS",
          new TrainDBMetaImpl.MetadataTable<MetaColumn>(MetaColumn.class) {
            @Override
            public Enumerator<MetaColumn> enumerator(
                final TrainDBMetaImpl meta) {
              final String catalog;
              try {
                catalog = meta.getConnection().getCatalog();
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
              return meta.tables(catalog)
                  .selectMany(meta::columns).enumerator();
            }
          },
          "TABLES",
          new TrainDBMetaImpl.MetadataTable<MetaTable>(MetaTable.class) {
            @Override
            public Enumerator<MetaTable> enumerator(TrainDBMetaImpl meta) {
              final String catalog;
              try {
                catalog = meta.getConnection().getCatalog();
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
              return meta.tables(catalog).enumerator();
            }
          });

  /**
   * Creates the data dictionary, also called the information schema. It is a
   * schema called "metadata" that contains tables "TABLES", "COLUMNS" etc.
   */
  private MetadataSchema() {
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return TABLE_MAP;
  }
}
