package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.SparkSession
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.rio.helpers.StatementCollector
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.spark.SparkSessionProvider
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers

import java.io.StringReader
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class WikibaseRDFDumpConverterUnitTest extends FlatSpec with SparkSessionProvider with Matchers {
  val wikidataPaths = Seq(
    this.getClass.getResource("small_dump_chunk.ttl").toURI.toString,
    this.getClass.getResource("lexeme_dump.ttl").toURI.toString
  )

  val commonsPaths = Seq(
    this.getClass.getResource("commons_dump.ttl").toURI.toString
  )

  "a wikidata rdf dump present" should "be converted as a table partition" in {
    TurtleImporter.importDump(Params(
      inputPath = wikidataPaths,
      outputTable = Some("rdf/date=20200602"),
      outputPath = None,
      numPartitions = 2,
      skolemizeBlankNodes = true,
      site = Site.wikidata), None)
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

    rdfTable.count() shouldEqual rdfTable.distinct().count()
  }

  "a commons rdf dump present" should "be converted as a table partition" in {
    TurtleImporter.importDump(Params(
      inputPath = commonsPaths,
      outputTable = Some("rdf/date=20200603"),
      outputPath = None,
      numPartitions = 2,
      skolemizeBlankNodes = true,
      site = Site.commons), None)
    val rdfTable = spark.read.table("rdf")
    val existingContext = rdfTable
      .filter("date = '20200603'")
      .select("context")
      .distinct()
      .collect()
      .map(_.getAs("context"): String)
      .toSet

    existingContext should contain allOf(
      "<https://commons.wikimedia.org/entity/M50827445>",
      "<https://commons.wikimedia.org/entity/M6580719>",
      "<http://wikiba.se/ontology#Dump>",
      "<http://wikiba.se/ontology#Value>")

    rdfTable.count() shouldEqual rdfTable.distinct().count()
  }

  "a rdf dump present" should "be converted as a parquet file" in {
    val rdfDir = newSparkSubDir("rdf_parquet")
    TurtleImporter.importDump(Params(
      inputPath = wikidataPaths,
      outputTable = None,
      outputPath = Some(rdfDir),
      numPartitions = 2,
      skolemizeBlankNodes = true,
      site = Site.wikidata), None)
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

    rdfDataframe.count() shouldEqual rdfDataframe.distinct().count()
  }

  "a rdf dump present" should "be converted as nt chunk files" in {
    val rdfDir = newSparkSubDir("nt_file")

    TurtleImporter.importDump(Params(
      inputPath = wikidataPaths,
      outputTable = None,
      outputPath = Some(rdfDir),
      outputFormat = "nt.gz",
      numPartitions = 2,
      skolemizeBlankNodes = true,
      site = Site.wikidata), None)

    val ntData = spark.read.text(rdfDir).collect().map(_.getString(0)).mkString("\n")

    val urisScheme = UrisSchemeFactory.WIKIDATA
    val collector = new StatementCollector()
    val parser = RDFParserSuppliers.defaultRdfParser().get(collector)
    parser.parse(new StringReader(ntData), "")
     val valueF = new ValueFactoryImpl()

     val s1 = valueF.createStatement(valueF.createURI("http://www.wikidata.org/entity/L4696"),
       valueF.createURI("http://www.wikidata.org/prop/direct/P5275"),
       valueF.createLiteral("189259")
     )
     val s2 = valueF.createStatement(valueF.createURI("http://www.wikidata.org/entity/Q8"),
       valueF.createURI("http://www.wikidata.org/prop/direct/P576"),
       valueF.createURI(urisScheme.wellKnownBNodeIRIPrefix() + "e39d2a834262fbd171919ab2c038c9fb")
     )
     collector.getStatements.asScala should contain allOf(s1, s2)
   }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val rdfData = newSparkSubDir("test_import")
    WikibaseRDFDumpConverterUnitTest.createTable("rdf", rdfData, spark)
  }
}

object WikibaseRDFDumpConverterUnitTest {
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
