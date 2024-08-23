package traindb.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrainDBWhatIfQueryTransformer {

  // Function to check if the query contains the WHATIF ~ TO clause
  public static boolean containsWhatIfToClause(String query) {
    String whatIfToPatternString = "WHATIF (.+) TO (\\d+\\.\\d+)";
    Pattern whatIfToPattern = Pattern.compile(whatIfToPatternString, Pattern.CASE_INSENSITIVE);
    Matcher matcher = whatIfToPattern.matcher(query.trim());
    return matcher.find();
  }

  // Function to print debug information
  public static void printDebugInfo(String asClause, String multiplier, String countColumn,
                                    String tableName, String whereCondition, String whatIfCondition) {
    System.out.println("asClause: " + asClause);
    System.out.println("multiplier: " + multiplier);
    System.out.println("countColumn: " + countColumn);
    System.out.println("tableName: " + tableName);
    System.out.println("whereCondition: " + whereCondition);
    System.out.println("whatIfCondition: " + whatIfCondition);
  }

  // Function to transform the query if it contains the WHATIF ~ TO clause
  public static String transformQuery(String inputQuery) {
    // Define the regex pattern for SELECT ~ FROM ~ WHERE ~ WHATIF ~ TO
    String whatIfToPatternString = "SELECT\\s+APPROXIMATE\\s+count\\((\\*|\\w+)\\)(?:\\s+as\\s+(\\w+))?\\s+FROM\\s+(\\w+\\.\\w+)(?:\\s+WHERE\\s+(.+?))?\\s+WHATIF\\s+(.+)\\s+TO\\s+(\\d+\\.\\d+)";
    Pattern whatIfToPattern = Pattern.compile(whatIfToPatternString, Pattern.CASE_INSENSITIVE);
    Matcher whatIfToMatcher = whatIfToPattern.matcher(inputQuery.trim());

    if (whatIfToMatcher.find()) {
      // Extract components from the input query
      String countColumn = whatIfToMatcher.group(1);  // e.g., "*" or "productid"
      String alias = whatIfToMatcher.group(2);   // e.g., "total" (could be null)
      String tableName = whatIfToMatcher.group(3);  // e.g., "myschema.sales"
      String whereCondition = whatIfToMatcher.group(4);  // e.g., "sales.price > 5" (could be null)
      String whatIfCondition = whatIfToMatcher.group(5);  // e.g., "sales.productid > 5 and sales.productid < 10"
      String multiplier = whatIfToMatcher.group(6); // e.g., "1.2"

      // Determine the "as" clause part for the output query
      String asClause = (alias != null) ? " as " + alias : "";

      // Check each part individually before passing to String.format
      //printDebugInfo(asClause, multiplier, countColumn, tableName, whereCondition, whatIfCondition);

      String outputQuery = String.format(
          "SELECT sum(cnt)%s\n" +  // asClause
              "FROM (\n" +
              "    SELECT APPROXIMATE %s * count(%s) as cnt\n" +  // multiplier, countColumn
              "    FROM %s\n" +  // tableName
              "    WHERE %s%s\n" +  // whereCondition and whatIfCondition
              "    UNION ALL\n" +
              "    SELECT APPROXIMATE count(%s) as cnt\n" +  // countColumn
              "    FROM %s\n" +  // tableName
              "    WHERE %s%s\n" +  // whereCondition and whatIfCondition for NOT clause
              ") as t",
          asClause != null ? " " + asClause : "",  // %s for asClause
          multiplier,  // %s for multiplier
          countColumn,  // %s for countColumn
          tableName,  // %s for tableName
          whereCondition != null ? whereCondition : "",  // %s for whereCondition (with null handling)
          whereCondition != null ? " AND " + whatIfCondition : whatIfCondition,  // %s for whatIfCondition (with null handling)
          countColumn,  // %s for countColumn
          tableName,  // %s for tableName
          whereCondition != null ? whereCondition : "",  // %s for whereCondition (with null handling)
          whereCondition != null ? " AND NOT (" + whatIfCondition + ")" : "NOT (" + whatIfCondition + ")"  // %s for NOT whatIfCondition (with null handling)
      );

      return outputQuery;
    }

    // If the query doesn't match the full WHATIF ~ TO pattern, return the input unchanged
    return inputQuery;
  }
}