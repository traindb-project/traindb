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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.CsvEnumerator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.schema.TrainDBSchema;
import traindb.schema.TrainDBTable;

/**
 * Table based on a file.
 */
public class TrainDBFileTable extends TrainDBTable implements QueryableTable, TranslatableTable {

  protected final Source source;
  private @Nullable List<RelDataType> fieldTypes;

  public TrainDBFileTable(String name, TrainDBSchema schema, MetaImpl.MetaTable tableDef,
                          RelDataType protoType, String uri) {
    super(name, schema, Schema.TableType.valueOf(tableDef.tableType), protoType);
    this.source = Sources.file(null, uri);
  }

  /**
   * Returns the field types of this table.
   */
  public List<RelDataType> getFieldTypes(RelDataTypeFactory typeFactory) {
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<>();
      for (RelDataTypeField field : getRowType(typeFactory).getFieldList()) {
        fieldTypes.add(field.getType());
      }
    }
    return fieldTypes;
  }

  /**
   * Returns whether the table represents a stream.
   */
  protected boolean isStream() {
    return false;
  }

  @Override
  public String toString() {
    return "TrainDBFileTable {" + getName() + "}";
  }

  /**
   * Returns an enumerable over a given projection of the fields.
   */
  @SuppressWarnings("unused") // called from generated code
  public Enumerable<Object> project(final DataContext root,
                                    final int[] fields) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    JavaTypeFactory typeFactory = root.getTypeFactory();
    return new CsvEnumerable<>(source, cancelFlag, getFieldTypes(typeFactory),
        ImmutableIntList.of(fields));
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = CsvEnumerator.identityList(fieldCount);
    return new CsvTableScan(context.getCluster(), relOptTable, this, fields);
  }
}
