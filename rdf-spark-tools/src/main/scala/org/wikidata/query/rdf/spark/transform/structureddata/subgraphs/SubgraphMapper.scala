package org.wikidata.query.rdf.spark.transform.structureddata.subgraphs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, lit, rand}
import org.wikidata.query.rdf.common.uri.{DefaultUrisScheme, PropertyType, UrisSchemeFactory}
import org.wikidata.query.rdf.spark.utils.{SparkUtils, SubgraphUtils}

/**
 * Maps items and triples to subgraphs. Here `item` means any entity in Wikidata.
 * An item is part a subgraph if it is instance of (P31) the subgraph entity.
 * All triples (direct or statements) originating from those items are the subgraph triples.
 *
 * @param wikidataTriples expected columns: context, subject, predicate, object
 */
class SubgraphMapper(wikidataTriples: DataFrame) {
  val scheme: DefaultUrisScheme = UrisSchemeFactory.WIKIDATA
  val p31: String = scheme.property(PropertyType.DIRECT) + "P31"

  /**
   * Lists all subgraphs in Wikidata
   *
   * @return spark dataframe containing list of all subgraphs with columns: subgraph, count
   */
  def getAllSubgraphs(): DataFrame = {
    wikidataTriples
      .filter(s"predicate='<$p31>'")
      .selectExpr("object as subgraph")
      .groupBy("subgraph")
      .count()
  }

  /**
   * Performs a right join when the left side is skewed. The right side must be small
   * enough to be collected and broadcast to executors.
   *
   * This cannot use a standard broadcast join. In a broadcast right join the left
   * dataset needs to be broadcasted, but in our case the right side is the one small
   * enough to be broadcasted.
   *
   * Instead we perform an inner join which can operate off a broadcast hash join, and
   * an anti join after pruning the left down to only the set of distinct subgraphs.
   *
   * This is specialized to the exact dataframes being used here, and is not a generic
   * right-join with skew implementation.
   */
  def rightSkewJoin(left: DataFrame, right: DataFrame): DataFrame = {
    // Using a broadcast join will avoid skew problems here, left wont even need to be shuffled.
    val inner = left.join(broadcast(right), Seq("subgraph"), "inner")

    // Then we need to union in the rows in the right dataset that don't match the left dataset.
    // Thankfully the right dataset is ~1MB and easy to deal with.

    // First we prepare the set of subgraphs that exist in the left dataset. We do a two-pass
    // distinct with a salt to deal with the skew, avoiding sending the largest subgraphs
    // to a single executor.
    val salt = (rand(0) * 10).cast("int").alias("salt")
    val leftSubgraphs = left
      .select(col("subgraph"), salt)
      .distinct()
      .drop("salt")
      .distinct()

    // Then perform an anti join from the right side. This will give us all subgraphs in right that
    // are not referenced in left. The anti join doesn't add in the extra null columns, so we manually
    // add the "item" column that would have been set to null in a standard right join.
    // scalastyle:off null
    val rightNotLeft = right.join(leftSubgraphs, right("subgraph").equalTo(leftSubgraphs("subgraph")), "anti")
      .withColumn("item", lit(null).cast(left.schema("item").dataType))
    // scalastyle:on null

    inner.union(rightNotLeft)
  }

  /**
   * Maps all items to one or more of the top subgraphs
   *
   * @param topSubgraphs expected columns: subgraph, count
   * @param minItems the minimum number of items a subgraph should have to be called a top_subgraph
   * @return spark dataframes with columns: subgraph, item
   */
  def getTopSubgraphItems(allSubgraphs: DataFrame, minItems: Long): DataFrame = {
    val topSubgraphs = allSubgraphs
      .filter(col("count") >= minItems)
      .drop("count")

    val filteredTriples = wikidataTriples
      .filter(s"predicate='<$p31>'")
      .selectExpr("object as subgraph", "subject as item")

    rightSkewJoin(filteredTriples, topSubgraphs)
  }

  /**
   * Maps all triples to one or more the top subgraphs. Does this by listing all triples under the items
   * that were identified as being part of a subgraph. Here predicate_code means the last part of the
   * predicate URI. For wikidata predicates, it would be the P-id.
   * See [[SubgraphUtils.extractItem]] for the extraction process.
   *
   * @param topSubgraphItems expected columns: subgraph, item
   * @return spark dataframes with columns: subgraph, subject, predicate, object, predicate_code
   */
  def getTopSubgraphTriples(topSubgraphItems: DataFrame): DataFrame = {
    wikidataTriples
      .select("context", "subject", "predicate", "object")
      .join(topSubgraphItems, wikidataTriples("context") === topSubgraphItems("item"), "inner")
      .drop("context")
      .withColumn("predicate_code", SubgraphUtils.extractItem(col("predicate"), lit("/")))
  }
}

object SubgraphMapper {

  /**
   * When the same data is referenced multiple times in a pipeline spark will recompute it
   * each time. Used to dataframes that need to both write to disk and be used in later
   * computation, so they are only computed once.
   */
  private def saveAndReload(df: DataFrame, path: String, numPartitions: Int)(implicit spark: SparkSession): DataFrame = {
    SparkUtils.saveTables((df.repartition(numPartitions), path) :: Nil)
    SparkUtils.readTablePartition(path)
  }
  /**
   * Reads input table, calls getSubgraphMapping(...) to extract subgraph mapping, and saves output tables
   */
  def extractAndSaveSubgraphMapping(wikidataTriplesPath: String,
                                    minItems: Long,
                                    allSubgraphsPath: String,
                                    topSubgraphItemsPath: String,
                                    topSubgraphTriplesPath: String): Unit = {

    implicit val spark: SparkSession = SparkUtils.getSparkSession("SubgraphMapper")

    val subgraphMapper = new SubgraphMapper(SparkUtils.readTablePartition(wikidataTriplesPath))

    // 1 partition since data is ~1mb
    val allSubgraphs = saveAndReload(subgraphMapper.getAllSubgraphs(), allSubgraphsPath, 1)

    // data is ~800mb
    val topSubgraphItems = saveAndReload(
      subgraphMapper
        .getTopSubgraphItems(allSubgraphs, minItems),
      topSubgraphItemsPath, 8)

    // data is ~340gb
    val topSubgraphTriples = subgraphMapper
      .getTopSubgraphTriples(topSubgraphItems)
      .repartition(3000)

    SparkUtils.saveTables((topSubgraphTriples, topSubgraphTriplesPath) :: Nil)
  }
}
