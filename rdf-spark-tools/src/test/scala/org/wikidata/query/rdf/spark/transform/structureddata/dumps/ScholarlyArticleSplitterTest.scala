package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.spark.SparkSessionProvider

class ScholarlyArticleSplitterTest extends AnyFlatSpec with SparkSessionProvider with Matchers {
  val importedFiles: Map[String, String] = Map(
    "Q42" -> this.getClass.getResource("Q42.ttl").toURI.toString,
    "Q37599471" -> this.getClass.getResource("Q37599471.ttl").toURI.toString,
    "Q_INTERSECTION" -> this.getClass.getResource("Q_INTERSECTION.ttl").toURI.toString
  )

  "ScholarlyArticleSplitter" should "be able to properly split a rdf dataset" in {
    importedFiles foreach {
      case (p, file) =>
        val dtPlaceholder = if (p == "Q_INTERSECTION") "19700101" else "20231027"
        TurtleImporter.importDump(Params(
          inputPath = Seq(file),
          outputTable = Some(s"rdf/date=$dtPlaceholder/entity=$p"),
          outputPath = None,
          numPartitions = 1,
          skolemizeBlankNodes = true,
          site = Site.wikidata), None)
    }
    ScholarlyArticleSplit.split(ScholarlyArticleSplitParams(
      inputPartition = s"rdf/date=20231027",
      outputPartitionParent = s"rdf_split/snapshot=20231027"
    ))
    assertSplitCorrectness(readSourceEntityTriples("Q37599471"), "scholarly_articles")
    assertSplitCorrectness(readSourceEntityTriples("Q42"), "wikidata_main")
    assertSharedTripleCorrectness(
      readSourceEntityTriples("Q_INTERSECTION"),
      "scholarly_articles",
      "wikidata_main",
      "Q_INTERSECTION")
  }

  private def assertSplitCorrectness(source: DataFrame, scope: String): Unit = {
    val split = readSplit(scope)
    withClue(s"$scope split should contain all source triples") { source.except(split).collect() shouldBe empty }
    withClue(s"$scope split should not have extra triples") { split.except(source).collect() shouldBe empty }
  }

  private def assertSharedTripleCorrectness(goal: DataFrame, scopeA: String, scopeB: String, exclude: String): Unit = {
    val partitionA = readSplit(scopeA)
    val partitionB = readSplit(scopeB)
    val goalWithoutSyntheticQID = goal.filter(s"subject not like '%$exclude%'")
    withClue(s"$scopeA and $scopeB intersection should be non-empty") {
      partitionA.intersect(partitionB) should not be empty
    }
    withClue(s"$scopeA and $scopeB intersection should be same size as intersection goal") {
      partitionA.intersect(partitionB).count shouldEqual goalWithoutSyntheticQID.count
    }
    withClue(s"$scopeA and $scopeB intersection minus intersection goal should have no leftovers") {
      partitionA.intersect(partitionB).except(goalWithoutSyntheticQID) shouldBe empty
    }
  }

  private def readSplit(scope: String) = {
    spark.read.table("rdf_split")
      .filter(s"scope='$scope'")
      .select("subject", "predicate", "object")
  }

  private def readSourceEntityTriples(entity: String) = {
    spark.read.table("rdf")
      .filter(s"entity = '$entity'")
      .select("subject", "predicate", "object")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val rdfData = newSparkSubDir("test_split_rdf")
    val rdfSplitData = newSparkSubDir("test_split_rdf_split")
    ScholarlyArticleSplitterTest.createTable("rdf", rdfData, spark)
    ScholarlyArticleSplitterTest.createSplitTable("rdf_split", rdfSplitData, spark)
  }
}

object ScholarlyArticleSplitterTest {
  def createTable(tableName: String, dir: String, spark: SparkSession): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (" +
      "context STRING," +
      "subject STRING," +
      "predicate STRING," +
      "entity STRING," +
      "object STRING," +
      "date STRING" +
      s""") USING parquet PARTITIONED BY (`date`, `entity`) LOCATION \"$dir\""""
    )
  }

  def createSplitTable(tableName: String, dir: String, spark: SparkSession): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (" +
      "context STRING," +
      "subject STRING," +
      "predicate STRING," +
      "object STRING," +
      "snapshot STRING," +
      "scope STRING" +
      s""") USING parquet PARTITIONED BY (`snapshot`, `scope`) LOCATION \"$dir\""""
    )
  }
}
