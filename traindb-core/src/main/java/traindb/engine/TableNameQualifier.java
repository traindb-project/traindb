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

package traindb.engine;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import traindb.schema.SchemaManager;

public final class TableNameQualifier {
  private TableNameQualifier() {
  }

  // translate table name identifier to full qualified name
  public static void toFullyQualifiedName(SchemaManager schemaManager, String defaultSchema,
                                          SqlNode query) {
    ArrayList<SqlIdentifier> tableIds = new ArrayList<>();
    query.accept(new SqlTableIdentifierFindVisitor(tableIds));

    for (SqlIdentifier tableId : tableIds) {
      List<String> fqn = schemaManager.toFullyQualifiedTableName(
          tableId.names, defaultSchema);
      tableId.setNames(fqn, null);
    }
  }
}
