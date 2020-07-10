package org.wikidata.query.rdf.spark

import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory

class EntityRevisionMapGeneratorUnitTest extends FlatSpec with SparkSessionProvider with Matchers with BeforeAndAfterEach {
  var csvFile: String = _
  val inputFormat = "parquet"

  val paths_import_20200101 = Seq(
    this.getClass.getResource ("small_dump_chunk.ttl").toURI.toString
  )

  val paths_import_20200102 = Seq(
    this.getClass.getResource ("small_dump_chunk.ttl").toURI.toString,
    this.getClass.getResource ("lexeme_dump.ttl").toURI.toString
  )

  "a rdf dump loaded into spark" should "be converted as an entity revision map" in {
    EntityRevisionMapGenerator.generateMap(spark, "rdf_for_rev_map/date=20200102", 2, csvFile, () => UrisSchemeFactory.WIKIDATA)
    val actualData = spark.read.format("csv").schema(EntityRevisionMapGenerator.schema).load(csvFile).take(3)
    actualData should contain allOf(
      Row.fromTuple("L4696", 766944915L),
      Row.fromTuple("Q8",1052652842L),
      Row.fromTuple("Q31",1065756426L)
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    csvFile = newSparkSubDir("entity_rev_map.csv")
    val rdfData = newSparkSubDir("test_import_form_rev_map")
    WikidataTurtleDumpConverterUnitTest.createTable("rdf_for_rev_map", rdfData, spark)
    WikidataTurtleDumpConverter.importDump(paths_import_20200101, skolemizeBlankNodes = true, WikidataTurtleDumpConverter.rowEncoder(),
      WikidataTurtleDumpConverter.getTableWriter("rdf_for_rev_map/date=20200101", 2, None))
    WikidataTurtleDumpConverter.importDump(paths_import_20200102, skolemizeBlankNodes = true, WikidataTurtleDumpConverter.rowEncoder(),
      WikidataTurtleDumpConverter.getTableWriter("rdf_for_rev_map/date=20200102", 2, None))
  }
}

