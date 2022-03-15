package org.wikidata.query.rdf.spark.transform.queries.subgraphsqueries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, lit}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{cleanLiteral, extractItem, uriToPrefix}

/**
 * Contains utilities to help map queries to subgraphs
 *
 * @param wikidataTriples    expected columns: context, subject, predicate, object
 * @param processedQueries   parsed SPARQL queries. expected columns: id, query, query_time, query_time_class, ua, q_info
 * @param topSubgraphTriples triples of the top subgraphs. Expected columns: subgraph, item, subject, predicate, object, predicate_code
 * @param filteringLimit     the percentage an entity should be used in a subgraph to be considered part of that subgraph
 */
class SubgraphQueryMapperUtils(wikidataTriples: DataFrame,
                               processedQueries: DataFrame,
                               topSubgraphTriples: DataFrame,
                               filteringLimit: Int) {

  // Intermediate Calculations: Extracts information from queries

  /**
   * Expand the wikidata (P/Q) items used in each query
   *
   * @return spark dataframe with columns: id, item
   */
  def getQueryWDnames(): DataFrame = {
    val queryWDnames = processedQueries
      .select(col("id"), explode(col("q_info.wikidatanames")))
      .selectExpr("id", "key as item")
      .distinct()

    queryWDnames
  }

  /**
   * Expand and extract the literals used in each query
   *
   * @return spark dataframe with columns: id, literal
   */
  def getQueryLiterals(): DataFrame = {
    val queryLiterals = processedQueries
      .select(col("id"), explode(col("q_info.nodes")))
      .selectExpr("id", "key as item")
      .filter(col("item").startsWith("NODE_LITERAL"))
      .select(
        col("id"),
        extractItem(col("item"), lit("NODE_LITERAL")).as("literal")
      )

    queryLiterals
  }

  /**
   * Expand and extract the URIs used in each query
   *
   * @return spark dataframe with columns: id, uri
   */
  def getQueryURIs(): DataFrame = {
    val queryUri = processedQueries
      .select(col("id"), explode(col("q_info.nodes")))
      .selectExpr("id", "key as item")
      .filter(col("item").startsWith("NODE_URI"))
      .select(
        col("id"),
        extractItem(col("item"), lit("NODE_URI")).as("uri")
      )

    queryUri
  }

  // Intermediate Calculations: Get Predicates, Literals, and URIs associated with each subgraphs

  /** Finds the count of predicate_code in each subgraph and in Wikidata, calculates the percentage,
   * and filters by those that are >= filteringLimit.
   * predicate_code is the last suffix of the predicate of the triple (i.e P123, rdf-schema#label etc)
   *
   * @return spark dataframe with columns: subgraph, predicate_code
   */
  def getSelectedPredicates(): DataFrame = {
    val selectedPredicates = topSubgraphTriples
      .groupBy("subgraph", "predicate_code")
      .count()
      .join(
        wikidataTriples.withColumn("predicate_code", extractItem(col("predicate"), lit("/")))
          .groupBy("predicate_code")
          .count()
          .withColumnRenamed("count", "total_count"),
        Seq("predicate_code"),
        "left"
      )
      .withColumn("percent", col("count") * 100.0 / col("total_count"))
      .filter(col("percent") >= filteringLimit)
      .select("subgraph", "predicate_code")

    selectedPredicates
  }

  /** Finds the number of times a URI is used as subject and/or object in Wikidata,
   * and sums these to find total usage.
   *
   * @return spark dataframe with columns: node, subCount, objCount, count
   */
  def getWikidataNodeMetrics(): DataFrame = {
    val wikidataSubjectCount = wikidataTriples
      .selectExpr("subject as node")
      .groupBy("node")
      .count()
      .withColumnRenamed("count", "subCount")

    val wikidataObjectCount = wikidataTriples
      .selectExpr("subject as node")
      .groupBy("node")
      .count()
      .withColumnRenamed("count", "objCount")

    val wikidataNodeMetrics = wikidataSubjectCount
      .join(wikidataObjectCount, Seq("node"), "outer")
      .na.fill(0)
      .withColumn("count", col("subCount") + col("objCount"))

    wikidataNodeMetrics
  }

  /** Finds the number of times a URI is used as subject and/or object,
   * and sums these to find total usage in each subgraph.
   *
   * @return spark dataframe with columns: node, subCount, objCount, subgraph_count
   */
  def getSubgraphNodeMetrics(): DataFrame = {
    val subgraphSubjectCount = topSubgraphTriples
      .groupBy("subgraph", "subject")
      .count()
      .selectExpr("subgraph", "subject as node", "count as subCount")

    val subgraphObjectCount = topSubgraphTriples
      .groupBy("subgraph", "object")
      .count()
      .selectExpr("subgraph", "object as node", "count as objCount")

    val subgraphNodeMetrics = subgraphSubjectCount
      .join(subgraphObjectCount, Seq("subgraph", "node"), "outer")
      .na.fill(0)
      .withColumn("subgraph_count", col("subCount") + col("objCount"))

    subgraphNodeMetrics
  }

  /** Uses total usage of nodes in each subgraph vs in entire Wikidata to find percent usage in
   * each subgraph. Then filters URIS by those that are >= filteringLimit. Only selects URIs.
   *
   * @param wikidataNodeMetrics dataframe with columns: node, subCount, objCount, count
   * @param subgraphNodeMetrics dataframe with columns: node, subCount, objCount, subgraph_count
   * @return spark dataframe with columns: subgraph, uri
   */
  def getSelectedURIs(wikidataNodeMetrics: DataFrame,
                      subgraphNodeMetrics: DataFrame): DataFrame = {

    val selectedUris = subgraphNodeMetrics
      .select("subgraph", "node", "subgraph_count")
      .join(wikidataNodeMetrics.select("node", "count"), Seq("node"), "left")
      .withColumn("percentage", col("subgraph_count") * 100.0 / col("count"))
      .filter(col("percentage") >= filteringLimit)
      .filter(col("node").startsWith("<"))
      .filter(col("node").endsWith(">"))
      .withColumn("uri", uriToPrefix(col("node")))
      .select("subgraph", "uri")
      .distinct()

    selectedUris
  }

  /** Uses total usage of nodes in each subgraph vs in entire Wikidata to find percent usage in
   * each subgraph. Then filters Literals by those that are >= filteringLimit. Only selects Literals.
   * Performs some extra calculations to remove quotation around literals, and get versions of
   * literals both with and without language and datatype extensions. For example: "ok"@en is ok and ok@en.
   *
   * @param wikidataNodeMetrics dataframe with columns: node, subCount, objCount, count
   * @param subgraphNodeMetrics dataframe with columns: node, subCount, objCount, subgraph_count
   * @return spark dataframe with columns: subgraph, literal
   */
  def getSelectedLiterals(wikidataNodeMetrics: DataFrame,
                          subgraphNodeMetrics: DataFrame): DataFrame = {

    val literalMetrics = subgraphNodeMetrics
      .select("subgraph", "node", "subgraph_count")
      .join(wikidataNodeMetrics.select("node", "count"), Seq("node"), "left")
      .filter(col("node").startsWith("\""))
      .withColumn("literal", cleanLiteral(col("node"), lit(true)))
      .withColumn("literalExt", cleanLiteral(col("node"), lit(false)))

    // direct distinct wont work because multiple languages can have same label
    // "ok"@en , "ok"@bn become: ok , ok@en , ok , ok@bn; the two ok - s will have different distributions,
    // and therefore wont combine when doing plain distinct. So, do groupBy and add the metrics together
    val selectedLiterals = literalMetrics
      .drop("node", "literalExt")
      .union(
        literalMetrics
          .drop("node", "literal")
          .withColumnRenamed("literalExt", "literal")
      )
      .groupBy("subgraph", "literal")
      .sum()
      .withColumnRenamed("sum(count)", "count")
      .withColumnRenamed("sum(subgraph_count)", "subgraph_count")
      .withColumn("percentage", col("subgraph_count") * 100.0 / col("count"))
      .filter(col("percentage") >= filteringLimit)
      .select("subgraph", "literal")
      .distinct()

    selectedLiterals
  }
}
