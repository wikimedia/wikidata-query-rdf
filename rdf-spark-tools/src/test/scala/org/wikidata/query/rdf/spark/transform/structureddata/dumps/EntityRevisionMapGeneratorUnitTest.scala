package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.spark.SparkSessionProvider

class EntityRevisionMapGeneratorUnitTest extends AnyFlatSpec with SparkSessionProvider with Matchers with BeforeAndAfterEach {
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
    WikibaseRDFDumpConverterUnitTest.createTable("rdf_for_rev_map", rdfData, spark)

    val params = Params(
      inputPath = paths_import_20200101,
      outputTable = Some("rdf_for_rev_map/date=20200101"),
      outputPath = None,
      numPartitions = 2,
      skolemizeBlankNodes = true,
      site = Site.wikidata)
    TurtleImporter.importDump(params, None)
    TurtleImporter.importDump(params.copy(inputPath = paths_import_20200102, outputTable = Some("rdf_for_rev_map/date=20200102")), None)
  }
}

