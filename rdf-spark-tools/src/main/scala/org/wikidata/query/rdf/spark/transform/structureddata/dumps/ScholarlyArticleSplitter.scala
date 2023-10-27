package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.wikidata.query.rdf.spark.utils.SparkUtils

object ScholarlyArticleSplitter {
  /** Splits an input partition into two output partitions, keeping shared pieces where needed.
   * Adapted from https://people.wikimedia.org/~andrewtavis-wmde/T342111_spark_sa_subgraph_metrics.html
   * for job wireup and more reliable execution. This additionally pulls values for references (not just entities).
   *
   * @param params input table partition spec and output table partition parent spec
   */
  def splitIntoPartitions(params: ScholarlyArticleSplitParams)(implicit spark: SparkSession): Unit = {
    val outPart = params.outputPartitionParent
    val baseTable = readBaseTable(params.inputPartition)
    // The following are re-used throughout this routine.
    // Spark operations run more reliably this way.
    val ontologyContextRefTriples = ontologyContextReferenceTriples(baseTable).cache()
    val ontologyContextValTriples = ontologyContextValueTriples(baseTable).cache()

    val scholEntities = entityQuads(baseTable, P31, Q13442814)
    val scholTriples = entitySubjectMatchesContext(scholEntities, baseTable)
    val refTripleObjectsOfScholTriples = distinctScholRefObjects(scholTriples).cache()
    val refTriplesAssociatedWithScholTriples = referenceTriplesAssociatedWithScholarlyArticles(
      refTripleObjectsOfScholTriples, ontologyContextRefTriples).cache()
    val scholTriplesAndTheirRefTriples = scholTriples union refTriplesAssociatedWithScholTriples
    val valTripleObjectsOfScholTriplesAndTheirRefTriples =
      distinctValTripleObjectsOfScholTriplesAndTheirRefTriples(scholTriplesAndTheirRefTriples)
    val valTriplesAssociatedWithScholTriplesAndTheirRefTriples =
      valueTriplesAssociatedWithScholTriplesAndTheirRefTriples(
        valTripleObjectsOfScholTriplesAndTheirRefTriples, ontologyContextValTriples)
    val scholGraph = scholTriples
      .union(valTriplesAssociatedWithScholTriplesAndTheirRefTriples)
      .union(refTriplesAssociatedWithScholTriples)

    val candidateTriplesOfNotScholTriples = baseTable.except(scholTriples)
    val valTripleObjectsOfScholTriples = distinctValTripleObjectsOfScholTriples(scholTriplesAndTheirRefTriples)
    val candidateTriplesOfNotScholTriplesAndTheirRefs = baseTable.except(scholTriplesAndTheirRefTriples)
    val candidateValTripleObjectsOfNotScholTriplesAndTheirRefs =
      distinctCandidateValTripleObjectsOfNotScholTriplesAndTheirRefs(candidateTriplesOfNotScholTriplesAndTheirRefs)
    val valTripleObjectsOnlyOfScholTriples =
      valueTripleObjectsOnlyOfScholTriples(valTripleObjectsOfScholTriples, candidateValTripleObjectsOfNotScholTriplesAndTheirRefs)
    val valTriplesOnlyAssociatedWithScholTriples = valueTriplesOnlyAssociatedWithScholarlyTriples(
      ontologyContextValTriples, valTripleObjectsOnlyOfScholTriples)
    val candidateRefTripleObjectsOfNotScholTriples =
      distinctCandidateRefTripleObjectsOfNotScholTriples(candidateTriplesOfNotScholTriples)
    val refTripleObjectsOnlyOfScholTriples =
      referenceTripleObjectsOnlyOfScholTriples(
        refTripleObjectsOfScholTriples, candidateRefTripleObjectsOfNotScholTriples)
    val refTriplesOnlyAssociatedWithScholTriples =
      referenceTriplesOnlyAssociatedWithScholTriples(ontologyContextRefTriples, refTripleObjectsOnlyOfScholTriples)
    val nonScholGraph = candidateTriplesOfNotScholTriples
      .except(valTriplesOnlyAssociatedWithScholTriples)
      .except(refTriplesOnlyAssociatedWithScholTriples)

    SparkUtils.insertIntoTablePartition(s"$outPart/scope=scholarly_articles", scholGraph)
    SparkUtils.insertIntoTablePartition(s"$outPart/scope=wikidata_main", nonScholGraph)
  }

  private def quadCols(prefix: Option[String] = None): List[Column] = prefix match {
    case Some(prefix) => QUAD_COL_NAMES.map(c => col(s"$prefix.$c")): List[Column]
    case None => QUAD_COL_NAMES.map(c => col(c)): List[Column]
  }

  private def entityQuads(from: DataFrame, predicateUri: String, objectUri: String) = {
    from
      .select(quadCols(): _*)
      .filter(col("predicate") === predicateUri)
      .filter(col("object") === objectUri)
      .as(ALIAS_SCHOL_ENTS)
  }

  private def readBaseTable(fromPartition: String)(implicit spark: SparkSession) = {
    SparkUtils.readTablePartition(fromPartition)
      .select(quadCols(): _*)
      .as(ALIAS_BASE)
  }

  private def ontologyContextValueTriples(baseTable: DataFrame) = {
    baseTable
      .filter(col("context") === "<http://wikiba.se/ontology#Value>")
      .as(ALIAS_ONT_CTX_VAL_TRIPS)
  }

  private def ontologyContextReferenceTriples(baseTable: DataFrame) = {
    baseTable
      .filter(col("context") === "<http://wikiba.se/ontology#Reference>")
      .as(ALIAS_ONT_CTX_REF_TRIPS)
  }
  private def entitySubjectMatchesContext(entities: DataFrame, baseTable: DataFrame) = {
    entities.join(
        baseTable,
        col(s"$ALIAS_SCHOL_ENTS.subject") === col(s"$ALIAS_BASE.context"))
      .select(quadCols(Some(ALIAS_BASE)): _*)
  }

  private def distinctObjects(from: DataFrame, startingWith: String, aliasOfColumn: String, alias: String): DataFrame = {
    val columnObject = "object"
    from
      .filter(col(columnObject) startsWith startingWith)
      .select(col(columnObject).alias(aliasOfColumn))
      .distinct()
      .as(alias)
  }
  private def distinctCandidateRefTripleObjectsOfNotScholTriples(candidates: DataFrame) = {
    distinctObjects(
      candidates,
      PREFIX_REF,
      COL_ALIAS_REF_OBJ,
      ALIAS_CANDIDATE_REF_TRIP_OBJ_OF_NOT_SCHOL_TRIPS)
  }

  private def distinctCandidateValTripleObjectsOfNotScholTriplesAndTheirRefs(candidates: DataFrame) = {
    distinctObjects(
      candidates,
      PREFIX_VAL,
      COL_ALIAS_VAL_OBJ,
      ALIAS_CANDIDATE_VAL_TRIP_OBJS_OF_NOT_SCHOL_TRIPS_AND_THEIR_REFS)
  }

  private def distinctValTripleObjectsOfScholTriples(from: DataFrame) = {
    distinctObjects(
      from,
      PREFIX_VAL,
      COL_ALIAS_VAL_OBJ,
      ALIAS_VAL_TRIP_OBJS_OF_SCHOL_TRIPS)
  }

  private def distinctValTripleObjectsOfScholTriplesAndTheirRefTriples(from: DataFrame) = {
    distinctObjects(
      from,
      PREFIX_VAL,
      COL_ALIAS_VAL_OBJ,
      ALIAS_VAL_TRIP_OBJS_OF_SCHOL_TRIPS_AND_THEIR_REF_TRIPS)
  }

  private def distinctScholRefObjects(from: DataFrame) = {
    distinctObjects(from, PREFIX_REF, COL_ALIAS_REF_OBJ, ALIAS_REF_TRIP_OBJS_OF_SCHOL_TRIPS)
  }

  private def referenceTriplesOnlyAssociatedWithScholTriples(ontologyContextTriples: DataFrame, referenceTripleObjects: DataFrame) = {
    ontologyContextTriples.join(
        referenceTripleObjects,
        col(s"$ALIAS_REF_TRIPS_OBJS_ONLY_OF_SCHOL.$COL_ALIAS_REF_OBJ") === col(s"$ALIAS_ONT_CTX_REF_TRIPS.subject"))
      .select(quadCols(Some(ALIAS_ONT_CTX_REF_TRIPS)): _*)
  }

  private def referenceTripleObjectsOnlyOfScholTriples(referenceTripleObjects: DataFrame, candidateReferenceTripleObjects: DataFrame) = {
    referenceTripleObjects.join(
        candidateReferenceTripleObjects,
        col(s"$ALIAS_REF_TRIP_OBJS_OF_SCHOL_TRIPS.$COL_ALIAS_REF_OBJ")
          === col(s"$ALIAS_CANDIDATE_REF_TRIP_OBJ_OF_NOT_SCHOL_TRIPS.$COL_ALIAS_REF_OBJ"),
        "left_anti")
      .as(ALIAS_REF_TRIPS_OBJS_ONLY_OF_SCHOL)
  }

  private def valueTriplesOnlyAssociatedWithScholarlyTriples(ontologyContextTriples: DataFrame, valueTripleObjects: DataFrame) = {
    ontologyContextTriples.join(
        valueTripleObjects,
        col(s"$ALIAS_VAL_TRIP_OBJS_ONLY_OF_SCHOL.$COL_ALIAS_VAL_OBJ")
          ===
          col(s"$ALIAS_ONT_CTX_VAL_TRIPS.subject"))
      .select(quadCols(Some(ALIAS_ONT_CTX_VAL_TRIPS)): _*)
  }

  private def valueTripleObjectsOnlyOfScholTriples(valueTripleObjects: DataFrame, candidateValueTripleObjects: DataFrame) = {
    valueTripleObjects.join(
        candidateValueTripleObjects,
        col(s"$ALIAS_VAL_TRIP_OBJS_OF_SCHOL_TRIPS.$COL_ALIAS_VAL_OBJ")
          ===
          col(s"$ALIAS_CANDIDATE_VAL_TRIP_OBJS_OF_NOT_SCHOL_TRIPS_AND_THEIR_REFS.$COL_ALIAS_VAL_OBJ"),
        "left_anti")
      .as(ALIAS_VAL_TRIP_OBJS_ONLY_OF_SCHOL)
  }

  private def valueTriplesAssociatedWithScholTriplesAndTheirRefTriples(valueTripleObjects: DataFrame, ontologyContextTriples: DataFrame) = {
    ontologyContextTriples
      .join(valueTripleObjects, col(s"$ALIAS_VAL_TRIP_OBJS_OF_SCHOL_TRIPS_AND_THEIR_REF_TRIPS.$COL_ALIAS_VAL_OBJ") === col(s"$ALIAS_ONT_CTX_VAL_TRIPS.subject"))
      .select(quadCols(Some(ALIAS_ONT_CTX_VAL_TRIPS)): _*)
  }

  private def referenceTriplesAssociatedWithScholarlyArticles(referenceTripleObjects: DataFrame, ontologyContextTriples: DataFrame) = {
    ontologyContextTriples.join(
        referenceTripleObjects,
        col(s"$ALIAS_REF_TRIP_OBJS_OF_SCHOL_TRIPS.$COL_ALIAS_REF_OBJ")
          ===
          col(s"$ALIAS_ONT_CTX_REF_TRIPS.subject"))
      .select(quadCols(Some(ALIAS_ONT_CTX_REF_TRIPS)): _*)
  }

  private val QUAD_COL_NAMES = List("subject", "predicate", "object", "context")
  private val ALIAS_BASE = "base"
  private val ALIAS_ONT_CTX_REF_TRIPS = "ocrt"
  private val ALIAS_ONT_CTX_VAL_TRIPS = "ocvt"
  private val COL_ALIAS_REF_OBJ = "reference_object"
  private val COL_ALIAS_VAL_OBJ = "value_object"
  private val P31 = "<http://www.wikidata.org/prop/direct/P31>"
  private val Q13442814 = "<http://www.wikidata.org/entity/Q13442814>"
  private val PREFIX_REF = "<http://www.wikidata.org/reference/"
  private val PREFIX_VAL = "<http://www.wikidata.org/value/"

  // We could really label these as anything as long as they were
  // different, but the variable names and mnemonics can help when
  // stepping through.
  private val ALIAS_SCHOL_ENTS = "sae"
  private val ALIAS_REF_TRIP_OBJS_OF_SCHOL_TRIPS = "rtoosat"
  private val ALIAS_VAL_TRIP_OBJS_OF_SCHOL_TRIPS_AND_THEIR_REF_TRIPS = "vtoosatatrt"
  private val ALIAS_VAL_TRIP_OBJS_OF_SCHOL_TRIPS = "vtoosat"
  private val ALIAS_CANDIDATE_VAL_TRIP_OBJS_OF_NOT_SCHOL_TRIPS_AND_THEIR_REFS = "cvtoonsatatr"
  private val ALIAS_VAL_TRIP_OBJS_ONLY_OF_SCHOL = "vtooosa"
  private val ALIAS_CANDIDATE_REF_TRIP_OBJ_OF_NOT_SCHOL_TRIPS = "crtoonsat"
  private val ALIAS_REF_TRIPS_OBJS_ONLY_OF_SCHOL = "rtooosa"
}
