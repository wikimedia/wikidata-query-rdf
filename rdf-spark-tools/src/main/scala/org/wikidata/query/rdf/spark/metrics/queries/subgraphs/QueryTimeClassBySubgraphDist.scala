package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{sparkDfColumnsToListOfStruct, sparkDfColumnsToMap}

object QueryTimeClassBySubgraphDist {

  /**
   * @param subgraphQueries        mapping of query to subgraph. Expected columns: id, subgraph, qid, item, predicate, uri, literal
   * @param processedQueries       parsed SPARQL queries. Expected columns: id, query, query_time, query_time_class, ua, q_info
   * @param subgraphQueriesInfo    all subgraphQueries and their info from processedQueries.
   *                               Expected columns: id, subgraph, query, query_time, query_time_class, ua, q_info
   * @param numOfSubgraphsPerQuery number of Queries that access `X` Number of Subgraphs together. Expected columns: id, subgraph_count
   * @return spark dataframe. Expected columns:
   *         query_time_class_subgraph_dist: array< struct< subgraph_count: bigint, query_time_class: map< string, bigint> > >
   */
  def getQueryTimeClassBySubgraphDist(subgraphQueries: DataFrame,
                                      processedQueries: DataFrame,
                                      subgraphQueriesInfo: DataFrame,
                                      numOfSubgraphsPerQuery: DataFrame): DataFrame = {

    // Number of subgraphs vs query time class
    // Join all >5 subgraphs
    val queryTimeClassByNumSubgraphs = numOfSubgraphsPerQuery
      .join(subgraphQueriesInfo, Seq("id"), "left")
      .withColumn(
        "subgraph_count",
        when(col("subgraph_count") >= 5, "4+").otherwise(col("subgraph_count"))
      )
      .groupBy("subgraph_count", "query_time_class")
      .count()

    val queryTimeClassOfNonSubgraphQueries = processedQueries
      .join(subgraphQueries, Seq("id"), "leftanti")
      .groupBy("query_time_class")
      .count()
      .withColumn("subgraph_count", lit("n/a"))

    // join the above two results
    // subgraph_count: 1,2,3,4,4+,n/a
    val queryTimeClassBySubgraphDist = queryTimeClassOfNonSubgraphQueries
      .unionByName(queryTimeClassByNumSubgraphs)

    val queryTimeClassBySubgraphDistAsStruct = sparkDfColumnsToListOfStruct(
      sparkDfColumnsToMap(
        queryTimeClassBySubgraphDist,
        "query_time_class",
        "count",
        "query_time_class",
        List("subgraph_count")
      ),
      List("subgraph_count", "query_time_class"),
      "query_time_class_subgraph_dist",
      List()

    )

    queryTimeClassBySubgraphDistAsStruct
  }
}
