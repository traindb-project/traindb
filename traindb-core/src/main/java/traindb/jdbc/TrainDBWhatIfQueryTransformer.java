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
  public static void printDebugInfo(String asClause, String multiplier, String aggregationFunction,
                                    String columnExpression, String tableName, String whereCondition, String whatIfCondition) {
    System.out.println("asClause: " + asClause);
    System.out.println("multiplier: " + multiplier);
    System.out.println("aggregationFunction: " + aggregationFunction);
    System.out.println("columnExpression: " + columnExpression);
    System.out.println("tableName: " + tableName);
    System.out.println("whereCondition: " + whereCondition);
    System.out.println("whatIfCondition: " + whatIfCondition);
  }

  // Helper function to generate appropriate alias based on the aggregation function
  public static String generateAlias(String aggregationFunction) {
    switch (aggregationFunction.toLowerCase()) {
      case "count":
        return "cnt_res";
      case "sum":
        return "sum_res";
      default:
        return "res";  // Fallback for any other aggregation functions
    }
  }

  // Function to transform the query if it contains the WHATIF ~ TO clause
  public static String transformQuery(String inputQuery) {
    // Define the regex pattern for SELECT ~ FROM ~ WHERE ~ WHATIF ~ TO
    String whatIfToPatternString = "SELECT\\s+APPROXIMATE\\s+(count|sum)\\(([^)]+)\\)(?:\\s+as\\s+(\\w+))?\\s+FROM\\s+(\\w+\\.\\w+)(?:\\s+WHERE\\s+(.+?))?\\s+WHATIF\\s+(.+)\\s+TO\\s+(\\d+\\.\\d+)";
    Pattern whatIfToPattern = Pattern.compile(whatIfToPatternString, Pattern.CASE_INSENSITIVE);
    Matcher whatIfToMatcher = whatIfToPattern.matcher(inputQuery.trim());

    if (whatIfToMatcher.find()) {
      // Extract components from the input query
      String aggregationFunction = whatIfToMatcher.group(1);  // e.g., "count" or "sum"
      String columnExpression = whatIfToMatcher.group(2);  // e.g., "*" or "productid * price * num"
      String alias = whatIfToMatcher.group(3);   // e.g., "total_sum" (could be null)
      String tableName = whatIfToMatcher.group(4);  // e.g., "myschema.sales"
      String whereCondition = whatIfToMatcher.group(5);  // e.g., "sales.price > 5" (could be null)
      String whatIfCondition = whatIfToMatcher.group(6);  // e.g., "sales.productid > 5 and sales.productid < 10"
      String multiplier = whatIfToMatcher.group(7); // e.g., "1.2"

      // Determine the "as" clause part for the output query
      String asClause = (alias != null) ? " as " + alias : "";

      // Check each part individually before passing to String.format
      //printDebugInfo(asClause, multiplier, aggregationFunction, columnExpression, tableName, whereCondition, whatIfCondition);

      // In your transformation code
      String outputQuery = String.format(
          "SELECT SUM(%s)%s\n" +  // Use dynamic alias
              "FROM (\n" +
              "    SELECT APPROXIMATE %s * %s(%s) AS %s\n" +  // multiplier, aggregationFunction, columnExpression, dynamic alias
              "    FROM %s\n" +  // tableName
              "    WHERE %s%s\n" +  // whereCondition and whatIfCondition
              "    UNION ALL\n" +
              "    SELECT APPROXIMATE %s(%s) AS %s\n" +  // aggregationFunction, columnExpression, dynamic alias
              "    FROM %s\n" +  // tableName
              "    WHERE %s%s\n" +  // whereCondition and whatIfCondition for NOT clause
              ") as t",
          generateAlias(aggregationFunction),  // Dynamic alias for sum in outer query
          asClause != null ? " " + asClause : "",  // %s for asClause
          multiplier,  // %s for multiplier
          aggregationFunction,  // %s for aggregationFunction
          columnExpression,  // %s for columnExpression
          generateAlias(aggregationFunction),  // Dynamic alias for inner query
          tableName,  // %s for tableName
          whereCondition != null ? whereCondition : "",  // %s for whereCondition (with null handling)
          whereCondition != null ? " AND " + whatIfCondition : whatIfCondition,  // %s for whatIfCondition (with null handling)
          aggregationFunction,  // %s for aggregationFunction
          columnExpression,  // %s for columnExpression
          generateAlias(aggregationFunction),  // Dynamic alias for inner query
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