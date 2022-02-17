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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import traindb.catalog.JDOCatalogStore;
import traindb.common.TrainDBConfiguration;


public class TrainDBServer implements Runnable {

  public static void main(String[] args) {
    new TrainDBServer().run();
  }
  public TrainDBServer() {
  }

  @Override
  public void run() {
    Logger log = Logger.get(TrainDBServer.class);

    ImmutableList.Builder<Module> modules = ImmutableList.builder();
    modules.add(
        new TrainDBServiceModule(),
        new NodeModule(),
        new HttpServerModule(),
        new EventModule(),
        new JsonModule(),
        new JaxrsModule()
    );

    Bootstrap app = new Bootstrap(modules.build());
    TrainDBConfiguration conf = new TrainDBConfiguration();
    try {
      Injector injector = app.initialize();

      log.info("TrainDBServer started.");
    }
    catch (Throwable e) {
      log.error(e);
      System.exit(1);
    }
  }
}
