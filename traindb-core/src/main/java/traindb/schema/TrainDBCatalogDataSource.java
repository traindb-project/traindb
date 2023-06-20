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
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlDialect;
import traindb.adapter.TrainDBStandaloneSqlDialect;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.pm.MSchema;
import traindb.common.TrainDBLogger;

public class TrainDBCatalogDataSource extends TrainDBDataSource {
  private static TrainDBLogger LOG = TrainDBLogger.getLogger(TrainDBCatalogDataSource.class);

  private final CatalogContext catalogContext;
  private final SqlDialect dialect;

  public TrainDBCatalogDataSource(CatalogContext catalogContext) {
    super();
    this.catalogContext = catalogContext;
    this.dialect = new TrainDBStandaloneSqlDialect();
    computeSubSchemaMap();
  }

  public void computeSubSchemaMap() {
    final ImmutableMap.Builder<String, Schema> builder = ImmutableMap.builder();
    try {
      for (MSchema mSchema : catalogContext.getSchemas()) {
        builder.put(mSchema.getSchemaName(), new TrainDBCatalogSchema(mSchema, this));
      }
      setSubSchemaMap(builder.build());
    } catch (CatalogException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SqlDialect getDialect() {
    return dialect;
  }
}
