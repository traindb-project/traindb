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

import traindb.common.TrainDBLogger;

public final class CancelContext {
  private static final TrainDBLogger LOG = TrainDBLogger.getLogger(CancelContext.class);

  private final Session session;
  private boolean cancellable;
  private volatile boolean canceled;

  CancelContext(Session session) {
    this.session = session;
  }

  public void enterCancel() {
    synchronized (this) {
      if (!cancellable) {
        cancellable = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("session(" + session.getId() + ") statement cancel enabled");
        }
      }
    }
  }

  public void exitCancel() {
    synchronized (this) {
      if (cancellable) {
        cancellable = false;
        canceled = false;
        if (LOG.isDebugEnabled()) {
          LOG.debug("session(" + session.getId() + ") statement cancel disabled");
        }
      }
    }
  }

  public void cancel() {
    synchronized (this) {
      if (cancellable) {
        canceled = true;
        LOG.info("statement cancel received for session(" + session.getId() + ")");
      }
    }
  }

  public boolean isCanceled() {
    if (!canceled) {
      return false;
    }

    synchronized (this) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("consume session(" + session.getId() + ") statement cancel");
      }
      canceled = false;
      return true;
    }
  }
}
