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

package traindb.adapter.python;

import au.com.bytecode.opencsv.CSVReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PythonMLAggregateEnumerable<E> extends AbstractEnumerable<E> {

  public final String csvResultPath;
  public final Map<String, SqlTypeName> fields;

  public PythonMLAggregateEnumerable(String csvResultPath, Map<String, SqlTypeName> fields) {
    this.csvResultPath = csvResultPath;
    this.fields = fields;
  }

  @Override
  public Enumerator<E> enumerator() {
    return (Enumerator<E>) Linq4j.enumerator(inferResult());
  }

  private List<?> inferResult() {
    if (fields.size() == 1) {
      return convertSingleColumn();
    }
    return convertArray();
  }

  private List<Object[]> convertArray() {
    List<Object[]> result = new ArrayList<>();
    try (CSVReader csvReader = new CSVReader(new FileReader(csvResultPath))) {
      String[] line;
      while ((line = csvReader.readNext()) != null) {
        Object[] row = new Object[fields.size()];
        int i = 0;
        for (Map.Entry<String, SqlTypeName> field : fields.entrySet()) {
          row[i] = convert(field.getValue(), line[i]);
          i++;
        }
        result.add(row);
      }
      return result;
    } catch (IOException exception) {
      return Collections.emptyList();
    } finally {
      File f = new File(csvResultPath);
      f.delete();
    }
  }

  private List<Object> convertSingleColumn() {
    List<Object> result = new ArrayList<>();
    try (CSVReader csvReader = new CSVReader(new FileReader(csvResultPath))) {
      String[] line;
      while ((line = csvReader.readNext()) != null) {
        Object row = convert(fields.entrySet().iterator().next().getValue(), line[0]);
        ;
        result.add(row);
      }
      return result;
    } catch (IOException exception) {
      return Collections.emptyList();
    } finally {
      File f = new File(csvResultPath);
      f.delete();
    }
  }

  private @Nullable Object convert(SqlTypeName fieldType, @Nullable String string) {
    switch (fieldType) {
      case BOOLEAN:
        if (string.length() == 0) {
          return null;
        }
        return Boolean.parseBoolean(string);
      case TINYINT:
        if (string.length() == 0) {
          return null;
        }
        return Byte.parseByte(string);
      case SMALLINT:
        if (string.length() == 0) {
          return null;
        }
        return Short.parseShort(string);
      case INTEGER:
        if (string.length() == 0) {
          return null;
        }
        return (int) Double.parseDouble(string);
      case BIGINT:
        if (string.length() == 0) {
          return null;
        }
        return (long) Double.parseDouble(string);
      case FLOAT:
        if (string.length() == 0) {
          return null;
        }
        return Float.parseFloat(string);
      case DOUBLE:
      case DECIMAL:
        if (string.length() == 0) {
          return null;
        }
        return Double.parseDouble(string);
      case DATE:
      case TIME:
      case TIMESTAMP:
      case VARCHAR:
      default:
        return string;
    }
  }

}