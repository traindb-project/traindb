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

package traindb.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;

public final class NetUtils {

  private NetUtils() {
  }

  /**
   * Util method to build socket addr from either:
   * <host>:<port>
   * <scheme>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target) {
    return createSocketAddr(target, null);
  }

  /**
   * Create an InetSocketAddress from the given target string. If the string
   * cannot be parsed correctly, the <code>configName</code> parameter is used
   * as part of the exception message, allowing the user to better diagnose
   * the misconfiguration.
   *
   * @param target     a string of either "host" or "host:port"
   * @param configName the name of the configuration from which
   *                   <code>target</code> was loaded. This is used in the
   *                   exception message in the case that parsing fails.
   */
  public static InetSocketAddress createSocketAddr(String target, String configName) {
    String helpText = "";
    if (configName != null) {
      helpText = " (configuration property '" + configName + "')";
    }

    if (target == null) {
      throw new IllegalArgumentException("Target address cannot be null." + helpText);
    }

    boolean hasScheme = target.contains("://");
    URI uri;
    try {
      uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://" + target);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target + helpText);
    }

    String path = uri.getPath();
    String host = uri.getHost();
    if ((!hasScheme && path != null && !path.isEmpty()) || host == null) {
      throw new IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target + helpText);
    }

    int port = uri.getPort();
    if (port < 0) {
      port = 0;
    }

    InetSocketAddress addr;
    try {
      addr = new InetSocketAddress(InetAddress.getByName(host), port);
    } catch (UnknownHostException e) {
      addr = InetSocketAddress.createUnresolved(host, port);
    }
    return addr;
  }

  /**
   * Compose a "host:port" string from the address.
   */
  public static String getHostPortString(InetSocketAddress addr) {
    return addr.getHostName() + ":" + addr.getPort();
  }
}
