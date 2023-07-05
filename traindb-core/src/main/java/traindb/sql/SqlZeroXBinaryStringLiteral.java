package traindb.sql;

import java.util.Objects;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.BitString;

public class SqlZeroXBinaryStringLiteral extends SqlBinaryStringLiteral {

  public SqlZeroXBinaryStringLiteral(
      BitString val,
      SqlParserPos pos) {
    super(val, pos);
  }

  private BitString getValueNonNull() {
    return (BitString) Objects.requireNonNull(value, "value");
  }

  @Override
  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal("0x" + getValueNonNull().toHexString());
  }
}
