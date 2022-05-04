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

import java.nio.channels.SocketChannel;
import org.apache.hadoop.conf.Configuration;
import traindb.catalog.CatalogStore;
import traindb.schema.SchemaManager;

public class SessionFactory {
  private final CatalogStore catalogStore;
  private final SchemaManager schemaManager;

  public SessionFactory(CatalogStore catalogStore, SchemaManager schemaManager) {
    this.catalogStore = catalogStore;
    this.schemaManager = schemaManager;
  }

  public Session createSession(SocketChannel clientChannel, Session.EventHandler sessEvtHandler) {
    return new Session(
        clientChannel, sessEvtHandler, catalogStore.getCatalogContext(), schemaManager);
  }
}
