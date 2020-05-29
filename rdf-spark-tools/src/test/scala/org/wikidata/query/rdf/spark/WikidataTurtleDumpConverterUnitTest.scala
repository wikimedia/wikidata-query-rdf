package org.wikidata.query.rdf.spark

import org.scalatest.{FlatSpec, Matchers}

class WikidataTurtleDumpConverterUnitTest extends FlatSpec with SparkSessionProvider with Matchers {
  var rdfData: String = _
  val paths = Seq(
    this.getClass.getResource("small_dump_chunk.ttl").toURI.toString,
    this.getClass.getResource("lexeme_dump.ttl").toURI.toString
  )

  "a rdf dump present" should "be converted as a parquet file" in {
    WikidataTurtleDumpConverter.importDump(spark, paths, 2, "parquet", outputPath = rdfData, skolemizeBlankNodes = true)
    val parquetFileDF = spark.read.parquet(rdfData)
    val existingContext = parquetFileDF
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

    parquetFileDF
      .filter("object = '<http://www.wikidata.org/.well-known/genid/e39d2a834262fbd171919ab2c038c9fb>'")
      .count() shouldEqual 1
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    rdfData = newSparkSubDir("test_import")
  }
}
