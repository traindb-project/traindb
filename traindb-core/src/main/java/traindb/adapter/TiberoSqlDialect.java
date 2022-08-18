
package traindb.adapter;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;

public class TiberoSqlDialect extends TrainDBSqlDialect {
    public static final Context DEFAULT_CONTEXT;
    public static final SqlDialect DEFAULT;

    static {
        DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
                .withIdentifierQuoteString("\"")
                .withUnquotedCasing(Casing.TO_LOWER)
                .withNullCollation(NullCollation.HIGH);
        DEFAULT = new KairosSqlDialect(DEFAULT_CONTEXT);
    }

    public TiberoSqlDialect() {
        super(DEFAULT_CONTEXT);
    }

    public TiberoSqlDialect(Context context) {
        super(context);
    }

    public SqlDialect getDefaultSqlDialect() {
        return DEFAULT;
    }

    @Override
    public boolean supportCatalogs() {
        return false;
    }
}
