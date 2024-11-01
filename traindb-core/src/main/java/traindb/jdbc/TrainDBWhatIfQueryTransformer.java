package traindb.jdbc;

import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Logger;
import java.util.logging.Level;

public class TrainDBWhatIfQueryTransformer {
  private static final Logger LOGGER = Logger.getLogger(TrainDBWhatIfQueryTransformer.class.getName());
  private static boolean isLoggingEnabled = false; // Toggle this flag to enable/disable logging
  static {
    if (isLoggingEnabled) {
      LOGGER.setLevel(Level.FINE);
      ConsoleHandler consoleHandler = new ConsoleHandler();
      consoleHandler.setLevel(Level.FINE);
      LOGGER.addHandler(consoleHandler);
      LOGGER.setUseParentHandlers(false);
    } else {
      LOGGER.setLevel(Level.OFF);  // Disable logging
    }
  }

  // Compile patterns once for better performance

  // WHATIF TO pattern
  private static final String WHATIF_CONDITION = "WHATIF (.+)";
  private static final String TO_MULTIPLIER = "TO (\\d+\\.\\d+)";
  private static final Pattern WHATIF_TO_PATTERN = Pattern.compile(
      WHATIF_CONDITION + "\\s+" + TO_MULTIPLIER,
      Pattern.CASE_INSENSITIVE
  );

  // Full query pattern
  private static final String PRE_SELECT = "(.*)";
  private static final String SELECT_APPROXIMATE = "SELECT\\s+APPROXIMATE\\s+";
  private static final String SELECT_COLUMNS = "(.*)";
  private static final String AGGREGATION_FUNCTION = "(count|sum|avg)";   // Aggregation functions: count, sum, avg
  private static final String COLUMN_EXPRESSION = "\\(([^)]+)\\)";        // Column or expression inside parentheses
  private static final String AS_CLAUSE = "(?:\\s+as\\s+(\\w+))?";        // Optional alias clause
  private static final String FROM_CLAUSE = "\\s+FROM\\s+(\\w+\\.\\w+)";  // Table name (schema.table)
  private static final String WHERE_CLAUSE = "(?:\\s+WHERE\\s+(.+?))?";   // Optional WHERE clause
  private static final String GROUPBY_CLAUSE = "(\\s+GROUP\\s+BY\\s+\\w+(\\s*,\\s*\\w+)*)?";   // Optional GROUPBY clause
  private static final String POST_SELECT = "(\\s+(.)*)?"; // Optional ORDER BY clause

  // Combine all parts to form the complete query pattern
  private static final Pattern QUERY_PATTERN = Pattern.compile(
      PRE_SELECT + SELECT_APPROXIMATE + SELECT_COLUMNS + AGGREGATION_FUNCTION + COLUMN_EXPRESSION
          + AS_CLAUSE + FROM_CLAUSE + WHERE_CLAUSE + "\\s+" + WHATIF_CONDITION + "\\s+"
          + TO_MULTIPLIER + GROUPBY_CLAUSE + POST_SELECT,
      Pattern.CASE_INSENSITIVE
  );

  // Use immutable map for better performance and thread-safety
  private static final Map<String, String> ALIAS_MAP = Map.of(
      "count", "cnt_res",
      "sum", "sum_res",
      "avg", "avg_res"
  );

  /**
   * Checks if the query contains a WHATIF TO clause.
   * @param query The input query string
   * @return true if the query contains a WHATIF TO clause, false otherwise
   */
  public static boolean containsWhatIfToClause(String query) {
    return WHATIF_TO_PATTERN.matcher(query.trim()).find();
  }

  /**
   * Generates an alias for the given aggregation function.
   * @param aggregationFunction The aggregation function (e.g., "count", "sum", "avg")
   * @return The corresponding alias
   */
  public static String generateAlias(String aggregationFunction) {
    return ALIAS_MAP.getOrDefault(aggregationFunction.toLowerCase(), "res");
  }

  /**
   * Transforms the query if it contains the WHATIF ~ TO clause.
   * @param inputQuery The input query string
   * @return The transformed query string, or the original query if no transformation is needed
   */
  public static String transformQuery(String inputQuery) {
    Matcher whatIfToMatcher = QUERY_PATTERN.matcher(inputQuery.trim());

    if (whatIfToMatcher.find()) {
      // Extract components from the input query
      String preSelect = whatIfToMatcher.group(1);
      String selectColumns = whatIfToMatcher.group(2);
      String aggregationFunction = whatIfToMatcher.group(3).toLowerCase();  // Convert to lowercase for easier comparison
      String columnExpression = whatIfToMatcher.group(4);     // The column or expression to aggregate
      String alias = whatIfToMatcher.group(5);                // The alias for the result, if specified
      String tableName = whatIfToMatcher.group(6);            // The name of the table to query
      String whereCondition = whatIfToMatcher.group(7);       // The WHERE condition, if specified
      String whatIfCondition = whatIfToMatcher.group(8);      // The WHATIF condition
      String multiplier = whatIfToMatcher.group(9);           // The multiplier for the WHATIF clause
      String groupby = whatIfToMatcher.group(10);
      String postSelect = whatIfToMatcher.group(12);

      if (groupby == null) {
        groupby = "";
      }
      if (postSelect == null) {
        postSelect = "";
      }

      // Determine the "as" clause for the output query
      String asClause = (alias != null) ? " as " + alias : "";

      // Log debug information about the extracted query components
      logDebugInfo(aggregationFunction, columnExpression, asClause, tableName, whereCondition,
          whatIfCondition, multiplier);

      // Check if the aggregation function is supported
      if (!ALIAS_MAP.containsKey(aggregationFunction)) {
        LOGGER.log(Level.WARNING,
            "Unsupported aggregation function: {0}. Only count, sum, and avg are supported.",
            aggregationFunction);
        return inputQuery;
      }

      String transformedQuery = buildTransformedQuery(aggregationFunction, selectColumns,
          columnExpression, asClause, tableName, whereCondition, whatIfCondition, multiplier,
          groupby);
      // Build and return the transformed query
      return preSelect + transformedQuery + groupby + postSelect;
    }

    // If the query doesn't match the WHATIF ~ TO pattern, return the input unchanged
    return inputQuery;
  }

  /**
   * Logs debug information about the extracted query components.
   *
   * @param aggregationFunction The aggregation function used (e.g., count, sum, avg)
   * @param columnExpression    The column or expression to aggregate
   * @param asClause            The "as" clause for the result, if specified
   * @param tableName           The name of the table to query
   * @param whereCondition      The WHERE condition, if specified
   * @param whatIfCondition     The WHATIF condition
   * @param multiplier          The multiplier for the WHATIF clause
   */
  private static void logDebugInfo(String aggregationFunction, String columnExpression, String asClause,
                                   String tableName, String whereCondition, String whatIfCondition, String multiplier) {
    LOGGER.log(Level.FINE, "Aggregation Function: {0}", aggregationFunction);
    LOGGER.log(Level.FINE, "Column Expression: {0}", columnExpression);
    LOGGER.log(Level.FINE, "As Clause: {0}", asClause != null ? asClause : "N/A");
    LOGGER.log(Level.FINE, "Table Name: {0}", tableName);
    LOGGER.log(Level.FINE, "Where Condition: {0}", whereCondition != null ? whereCondition : "N/A");
    LOGGER.log(Level.FINE, "WhatIf Condition: {0}", whatIfCondition);
    LOGGER.log(Level.FINE, "Multiplier: {0}", multiplier);
  }

  /**
   * Builds the transformed query string.
   * @param aggregationFunction The aggregation function used
   * @param columnExpression The column or expression to aggregate
   * @param asClause The alias clause for the result, if any
   * @param tableName The name of the table to query
   * @param whereCondition The WHERE condition, if any
   * @param whatIfCondition The WHATIF condition
   * @param multiplier The multiplier for the WHATIF clause
   * @return The transformed query string
   */
  private static String buildTransformedQuery(String aggregationFunction, String selectColumns,
                                              String columnExpression, String asClause,
                                              String tableName, String whereCondition,
                                              String whatIfCondition, String multiplier,
                                              String groupby) {
    String baseCondition = whereCondition != null ? whereCondition : "";
    String whatIfWithBase = whereCondition != null ? " AND " + whatIfCondition : whatIfCondition;
    String notWhatIfWithBase = whereCondition != null ? " AND NOT (" + whatIfCondition + ")" : "NOT (" + whatIfCondition + ")";

    StringBuilder queryBuilder = new StringBuilder();

    String innerSelect;
    String outerSelect;

    switch (aggregationFunction) {
      case "count":
      case "sum":
        String alias = generateAlias(aggregationFunction);
        innerSelect = String.format("SELECT APPROXIMATE %s %%s %s(%s) AS %s",
            selectColumns, aggregationFunction, columnExpression, alias);
        outerSelect = String.format("SELECT %s SUM(%s)%s", selectColumns, alias, asClause);
        break;
      case "avg":
        innerSelect = String.format("SELECT APPROXIMATE %s %%s SUM(%s) AS sum_res, %%s COUNT(%s) AS cnt_res",
            selectColumns, columnExpression, columnExpression);
        outerSelect = String.format("SELECT %s SUM(sum_res) / SUM(cnt_res)%s",
            selectColumns, asClause);
        break;
      default:
        // This case should never be reached due to the check in transformQuery, but included for completeness
        LOGGER.log(Level.SEVERE, "Unexpected aggregation function: {0}", aggregationFunction);
        return null;
    }

    queryBuilder.append(outerSelect)
        .append("\nFROM (\n")
        .append(String.format(innerSelect, multiplier + " *", multiplier + " *"))
        .append("\n    FROM ").append(tableName)
        .append("\n    WHERE ").append(baseCondition).append(whatIfWithBase).append(groupby)
        .append("\n    UNION ALL\n")
        .append(String.format(innerSelect, "", ""))
        .append("\n    FROM ").append(tableName)
        .append("\n    WHERE ").append(baseCondition).append(notWhatIfWithBase).append(groupby)
        .append("\n) as t");

    return queryBuilder.toString();
  }
}