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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Random;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import traindb.catalog.CatalogContext;
import traindb.schema.SchemaManager;

public final class Session implements Runnable {
  private static final Log LOG = LogFactory.getLog(Session.class);
  private static final ThreadLocal<Session> LOCAL_SESSION = new ThreadLocal<>();
  private final CancelContext cancelContext;
  private final int sessionId;

  private final SocketChannel clientChannel;
  private final EventHandler eventHandler;

  private final CatalogContext catalogContext;
  private final SchemaManager schemaManager;

  Session(SocketChannel clientChannel, EventHandler eventHandler,
          CatalogContext catalogContext, SchemaManager schemaManager) {
    sessionId = new Random(this.hashCode()).nextInt();
    cancelContext = new CancelContext(this);
    this.clientChannel = clientChannel;
    this.eventHandler = eventHandler;

    this.catalogContext = catalogContext;
    this.schemaManager = schemaManager;

    // TODO: create TrainDBQueryEngine
  }

  public static Session currentSession() {
    Session currSess = LOCAL_SESSION.get();
    if (currSess == null) {
      throw new RuntimeException("current session does not exist");
    }

    return LOCAL_SESSION.get();
  }

  public int getId() {
    return sessionId;
  }

  public boolean isCanceled() {
    return cancelContext.isCanceled();
  }

  @Override
  public void run() {
    LOCAL_SESSION.set(this);
    try {
      messageLoop();
    } catch (Exception e) {
      LOG.fatal(ExceptionUtils.getStackTrace(e));
    }
    LOCAL_SESSION.remove();

    close();
  }

  private void messageLoop() throws Exception {
    while (true) {
      try {
        // TODO: handle messages
      } catch (Exception e) {

      }
    }
  }

  void reject() {
    close();
  }

  void close() {
    try {
      clientChannel.close();
    } catch (IOException e) {
      LOG.info(ExceptionUtils.getStackTrace(e));
    }

    eventHandler.onClose(this);
  }

  void cancel() {
    cancelContext.cancel();
  }

  interface EventHandler {
    void onClose(Session session);

    void onCancel(int sessionId);
  }
}
