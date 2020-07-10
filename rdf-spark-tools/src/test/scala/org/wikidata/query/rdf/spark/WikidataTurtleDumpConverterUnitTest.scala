package org.wikidata.query.rdf.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class WikidataTurtleDumpConverterUnitTest extends FlatSpec with SparkSessionProvider with Matchers {
  val paths = Seq(
    this.getClass.getResource("small_dump_chunk.ttl").toURI.toString,
    this.getClass.getResource("lexeme_dump.ttl").toURI.toString
  )


  "a rdf dump present" should "be converted as a table partition" in {
    val writer = WikidataTurtleDumpConverter.getTableWriter("rdf/date=20200602", 2, None)
    WikidataTurtleDumpConverter.importDump(paths, skolemizeBlankNodes = true,
      WikidataTurtleDumpConverter.rowEncoder(), writer)
    val rdfTable = spark.read.table("rdf")
    val existingContext = rdfTable
      .filter("date = '20200602'")
      .select("context")
      .distinct()
      .collect()
      .map(_.getAs("context"): String)
      .toSet

    existingContext should contain allOf(
      "<http://www.wikidata.org/entity/Q31>",
      "<http://www.wikidata.org/entity/Q8>",
      "<http://www.wikidata.org/entity/L4696>",
      "<http://wikiba.se/ontology#Reference>",
      "<http://wikiba.se/ontology#Value>",
      "<http://wikiba.se/ontology#Dump>")

    rdfTable
      .filter("object = '<http://www.wikidata.org/.well-known/genid/e39d2a834262fbd171919ab2c038c9fb>'")
      .count() shouldEqual 1
  }

  "a rdf dump present" should "be converted as a parquet file" in {
    val rdfDir = newSparkSubDir("rdf_parquet")
    val writer = WikidataTurtleDumpConverter.getDirectoryWriter(rdfDir, "parquet", 2)
    WikidataTurtleDumpConverter.importDump(paths, skolemizeBlankNodes = true,
      WikidataTurtleDumpConverter.rowEncoder(), writer)
    val rdfDataframe = spark.read.parquet(rdfDir)
    val existingContext = rdfDataframe
      .select("context")
      .distinct()
      .collect()
      .map(_.getAs("context"): String)
      .toSet

    existingContext should contain allOf(
      "<http://www.wikidata.org/entity/Q31>",
      "<http://www.wikidata.org/entity/Q8>",
      "<http://www.wikidata.org/entity/L4696>",
      "<http://wikiba.se/ontology#Reference>",
      "<http://wikiba.se/ontology#Value>",
      "<http://wikiba.se/ontology#Dump>")

    rdfDataframe
      .filter("object = '<http://www.wikidata.org/.well-known/genid/e39d2a834262fbd171919ab2c038c9fb>'")
      .count() shouldEqual 1
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val rdfData = newSparkSubDir("test_import")
    WikidataTurtleDumpConverterUnitTest.createTable("rdf", rdfData, spark)
  }
}

object WikidataTurtleDumpConverterUnitTest {
  def createTable(tableName: String, dir: String, spark: SparkSession): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (" +
      "context STRING," +
      "subject STRING," +
      "predicate STRING," +
      "object STRING," +
      "date STRING" +
      s""") USING parquet PARTITIONED BY (`date`) LOCATION \"$dir\""""
    )
  }
}
