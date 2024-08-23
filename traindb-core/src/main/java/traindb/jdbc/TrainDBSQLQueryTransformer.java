package traindb.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrainDBSQLQueryTransformer {

  // Function to check if the query contains the WITH ~ TO clause
  public static boolean containsWithToClause(String query) {
    String withToPatternString = "WITH (.+) TO (\\d+\\.\\d+)";
    Pattern withToPattern = Pattern.compile(withToPatternString, Pattern.CASE_INSENSITIVE);
    Matcher matcher = withToPattern.matcher(query.trim());
    return matcher.find();
  }

  // Function to extract the "as" clause if it exists
  public static String extractAsClause(String query) {
    String asPatternString = "\\bas\\s+(\\w+)";
    Pattern asPattern = Pattern.compile(asPatternString, Pattern.CASE_INSENSITIVE);
    Matcher matcher = asPattern.matcher(query.trim());
    if (matcher.find()) {
      return matcher.group(1);  // Returns the alias, e.g., "order_count"
    }
    return null;  // No "as" clause found
  }

  // Function to transform the query if it contains the WITH ~ TO clause
  public static String transformQuery(String inputQuery) {
    // Define the regex pattern for WITH ~ TO case, now supporting count(column)
    String withToPatternString = "SELECT\\s+APPROXIMATE\\s+count\\((\\*|\\w+)\\)(?:\\s+as\\s+(\\w+))?\\s+FROM\\s+(\\w+\\.\\w+)\\s+WITH\\s+(.+)\\s+TO\\s+(\\d+\\.\\d+)";
    Pattern withToPattern = Pattern.compile(withToPatternString, Pattern.CASE_INSENSITIVE);
    Matcher withToMatcher = withToPattern.matcher(inputQuery.trim());

    if (withToMatcher.find()) {
      // Extract components from the input query
      String countColumn = withToMatcher.group(1);  // e.g., "*" or "column"
      String alias = withToMatcher.group(2);   // e.g., "order_count" (could be null)
      String tableName = withToMatcher.group(3);  // e.g., "Instacart.order_products"
      String condition = withToMatcher.group(4);  // e.g., "reordered = 1"
      String multiplier = withToMatcher.group(5); // e.g., "1.1"

      // Determine the "as" clause part for the output query
      String asClause = (alias != null) ? " as " + alias : "";

      // Build the output query
      String outputQuery = String.format(
          "SELECT sum(cnt)%s\n" +
              "FROM (\n" +
              "    SELECT APPROXIMATE %s * count(%s) as cnt\n" +
              "    FROM %s\n" +
              "    WHERE %s\n" +
              "    UNION ALL\n" +
              "    SELECT APPROXIMATE count(%s) as cnt\n" +
              "    FROM %s\n" +
              "    WHERE NOT (%s)\n" +
              ") as t",
          asClause, multiplier, countColumn, tableName, condition, countColumn, tableName, condition
      );

      return outputQuery;
    }

    // If the query doesn't match the full WITH ~ TO pattern, return the input unchanged
    return inputQuery;
  }
}