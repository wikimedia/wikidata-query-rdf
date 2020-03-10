package org.wikidata.query.rdf.spark

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WikidataTurtleDumpConverterUnitTest extends FlatSpec with SparkSessionProvider with Matchers with BeforeAndAfter {
  val path = this.getClass.getResource("small_dump_chunk.ttl").toURI.toString

  "a rdf dump present" should "be converted as a parquet file" in {
    WikidataTurtleDumpConverter.importDump(spark, path, 2, "parquet", "test_import")
    val parquetFileDF = spark.read.parquet("test_import")
    val existingContext = parquetFileDF
      .select("context")
      .distinct()
      .collect()
      .map(_.getAs("context"): String)
      .toSet

    existingContext should contain allOf(
      "<http://www.wikidata.org/entity/Q31>",
      "<http://www.wikidata.org/entity/Q8>",
      "<http://wikiba.se/ontology#Reference>",
      "<http://wikiba.se/ontology#Value>",
      "<http://wikiba.se/ontology#Dump>")
  }

  before {
    spark.sparkContext.addFile(path)
  }
}
