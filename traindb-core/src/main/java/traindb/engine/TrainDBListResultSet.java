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

import com.google.common.base.Optional;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class TrainDBListResultSet {

  private List<String> header = new ArrayList<>();
  private Optional<List<List<Object>>> result;
  int cursor = -1;

  public TrainDBListResultSet(List<String> header, List<List<Object>> result) {
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      this.header = header;
      this.result = Optional.of(result);
    }
  }

  public static TrainDBListResultSet empty() {
    return new TrainDBListResultSet(null, null);
  }

  public boolean isEmpty() {
    return !result.isPresent();
  }

  public int getColumnCount() {
    if (result.isPresent() == false || result.get().isEmpty()) {
      return 0;
    } else {
      Object o = result.get().get(0);
      if (o instanceof List) {
        return ((List) o).size();
      } else {
        return 1;
      }
    }
  }

  public String getColumnName(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return header.get(index);
    }
  }

  public int getColumnType(int index) {
    if (result.isPresent() == false || result.get().isEmpty()) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object o = (result.get().get(0)).get(index);
      if (o instanceof String) {
        return Types.VARCHAR;
      } else if (o instanceof Integer) {
        return Types.INTEGER;
      } else {
        return Types.JAVA_OBJECT;
      }
    }
  }

  public long getRowCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().size();
    }
  }

  public Object getValue(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().get(cursor).get(index);
    }
  }

  public boolean next() {
    if (result.isPresent() == false) {
      return false;
    } else {
      if (cursor < getRowCount() - 1) {
        cursor++;
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean hasNext() {
    if (result.isPresent() == false) {
      return false;
    } else {
      return cursor < getRowCount() - 1;
    }
  }

  public void rewind() {
    cursor = -1;
  }
}
