package org.wikidata.query.rdf.spark

import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory

class EntityRevisionMapGeneratorUnitTest extends FlatSpec with SparkSessionProvider with Matchers with BeforeAndAfterEach {
  var rdfData: String = _
  var csvFile: String = _
  val inputFormat = "parquet"

  val paths = Seq(
    this.getClass.getResource ("small_dump_chunk.ttl").toURI.toString,
    this.getClass.getResource ("lexeme_dump.ttl").toURI.toString
  )

  "a rdf dump loaded into spark" should "be converted as an entity revision map" in {
    EntityRevisionMapGenerator.generateMap(spark, rdfData, inputFormat, 2, csvFile, () => UrisSchemeFactory.WIKIDATA)
    val actualData = spark.read.format("csv").schema(EntityRevisionMapGenerator.schema).load(csvFile).take(3)
    actualData should contain allOf(
      Row.fromTuple("L4696", 766944915L),
      Row.fromTuple("Q8",1052652842L),
      Row.fromTuple("Q31",1065756426L)
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    rdfData = newSparkSubDir("test_import_dump")
    csvFile = newSparkSubDir("entity_rev_map.csv")
    WikidataTurtleDumpConverter.importDump(spark, paths, 2, "parquet", rdfData)
  }
}

