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

package traindb.adapter.file;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;

public class CsvEnumerable<E> extends AbstractEnumerable<E> {

  Source source;
  AtomicBoolean cancelFlag;
  List<RelDataType> fieldTypes;
  List<Integer> fields;

  public CsvEnumerable(Source source, AtomicBoolean cancelFlag, List<RelDataType> fieldTypes,
                       List<Integer> fields) {
    this.source = source;
    this.cancelFlag = cancelFlag;
    this.fieldTypes = fieldTypes;
    this.fields = fields;
  }

  @Override
  public Enumerator<E> enumerator() {
    return new CsvEnumerator<>(source, cancelFlag, fieldTypes, fields);
  }

}