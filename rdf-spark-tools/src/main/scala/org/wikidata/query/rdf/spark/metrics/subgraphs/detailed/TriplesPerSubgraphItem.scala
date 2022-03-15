package org.wikidata.query.rdf.spark.metrics.subgraphs.detailed

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.common.uri.{DefaultUrisScheme, PropertyType, UrisSchemeFactory}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.getPercentileExpr

object TriplesPerSubgraphItem {

  /**
   * Gets metrics per subgraph which includes number of triples, items, predicates etc.
   *
   * @param topSubgraphTriples expected columns: subgraph, item, subject, predicate, object, predicate_code
   * @return four spark dataframes:
   *         - triplesPerItem expected columns: subgraph, triples_per_item_percentiles, triples_per_item_mean
   *         - numDirectTriples expected columns: subgraph, num_direct_triples
   *         - numStatements expected columns: subgraph, num_statements
   *         - numFullStatements expected columns: subgraph, num_statement_triples
   */
  def getTriplesPerSubgraphItem(topSubgraphTriples: DataFrame)
                               (implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    val scheme: DefaultUrisScheme = UrisSchemeFactory.WIKIDATA
    val claimPropertyPrefix = scheme.property(PropertyType.CLAIM)

    val triplesPerItem = topSubgraphTriples
      .groupBy("subgraph", "item")
      .count()
      .groupBy("subgraph")
      .agg(expr(getPercentileExpr("count", "triples_per_item_percentiles")),
        expr("mean(count) as triples_per_item_mean"))

    // Number of triples per subgraph with direct vs indirect triples
    // This includes both wikidata properties and non-wikidata predicates like labels and descriptions
    val numDirectTriples = topSubgraphTriples.filter("item = subject")
      .filter(s"predicate not like '<${claimPropertyPrefix}P%'")
      .groupBy("subgraph")
      .count()
      .withColumnRenamed("count", "num_direct_triples")

    // Number of statement triples
    val statements = topSubgraphTriples.filter("item = subject")
      .filter(s"predicate like '<${claimPropertyPrefix}P%'")
    val numStatements = statements
      .groupBy("subgraph")
      .count()
      .withColumnRenamed("count", "num_statements")

    val statementObjects = statements.selectExpr("object as statement_object")
    val numFullStatements = statementObjects
      .join(
        topSubgraphTriples,
        statementObjects("statement_object") === topSubgraphTriples("subject"),
        "inner")
      .drop("statement_object")
      .groupBy("subgraph")
      .count()
      .withColumnRenamed("count", "num_statement_triples")

    (triplesPerItem, numDirectTriples, numStatements, numFullStatements)
  }
}
