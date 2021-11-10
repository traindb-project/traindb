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
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.connection.DbmsQueryResultMetaData;

public class TrainDBResultFromListData extends VerdictSingleResult {

  List<String> fieldsName = new ArrayList<>();
  int cursor = -1;
  private Optional<List<List<Object>>> result;
  // used to support wasnull()
  private Object lastValueRead;

  public TrainDBResultFromListData() {
  }

  public TrainDBResultFromListData(List<String> header, List<List<Object>> result) {
    super();
    if (result == null) {
      this.result = Optional.absent();
    } else {
      fieldsName = header;
      this.result = Optional.of(result);
    }
  }

  public static TrainDBResultFromListData empty() {
    return new TrainDBResultFromListData(null, null);
  }

  public boolean isEmpty() {
    return !result.isPresent();
  }

  public DbmsQueryResultMetaData getMetaData() {
    return null;
  }

  @Override
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

  @Override
  public String getColumnName(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return fieldsName.get(index);
    }
  }

  public int getColumnType(int index) {
    if (result.isPresent() == false || result.get().isEmpty()) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      Object o = (result.get().get(0)).get(index);
      if (o instanceof String) {
        return DataTypeConverter.typeInt("varchar");
      } else if (o instanceof Integer) {
        return DataTypeConverter.typeInt("int");
      } else {
        return Types.JAVA_OBJECT;
      }
    }
  }

  public String getColumnTypeNamePy(int index) {
    return DataTypeConverter.typeName(getColumnType(index));
  }

  public long getRowCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().size();
    }
  }

  @Override
  public Object getValue(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().get(cursor).get(index);
    }
  }

  public boolean wasNull() {
    return lastValueRead == null;
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
