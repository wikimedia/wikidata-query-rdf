package org.wikidata.query.rdf.spark.metrics.queries

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.model.{Value, ValueFactory}
import org.openrdf.query.impl.{BindingImpl, ListBindingSet, MapBindingSet, TupleQueryResultImpl}
import org.openrdf.query.{BindingSet, TupleQueryResult}
import org.openrdf.rio.ntriples.NTriplesUtil
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.spark.SparkSessionProvider
import org.wikidata.query.rdf.tool.exception.FatalException
import org.wikidata.query.rdf.tool.rdf.client.RdfClient

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.time.Instant
import java.{lang, util}
import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.Random

class QueryResultRecorderTest extends AnyFlatSpec with SparkSessionProvider with Matchers with MockFactory with BeforeAndAfterAll with BeforeAndAfterEach {
  val valueFactory: ValueFactory = new ValueFactoryImpl()
  val client: RdfClient = mock[RdfClient]
  val recorder: QueryResultRecorder = new QueryResultRecorder(() => client)
  var random: util.Random = new util.Random(QueryResultRecorderTest.seed)
  val scalaRandom: Random = new Random(random)

  override def beforeEach(): Unit = {
    super.beforeEach()
    random = new util.Random(QueryResultRecorderTest.seed)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    LoggerFactory.getLogger(this.getClass.getName).info("Using seed, {}", QueryResultRecorderTest.seed)
  }

  "QueryResultRecorder" should "produce a QueryResultResult" in {
    val (varNames, bindingSets) = randomResults(random.nextInt(200) + 1, random.nextInt(20) + 1)
    val tupleQueryResult: TupleQueryResult = toTupeQueryResult(varNames, bindingSets)
    (client.query(_: String)) expects "query 1" returns tupleQueryResult once()
    val result = recorder.query("query 1")
    result.success shouldBe true
    result.errorMessage shouldBe None
    result.resultSize shouldBe Some(bindingSets.size)
    result.exactHash should not be None
    result.reorderedHash should not be None
    result.results should not be None
    result.results map { _.toList } map { decodeResult } foreach {
      case (decodedVarnames, decodedBindingSets) =>
        decodedVarnames should contain only (varNames: _*)
        decodedBindingSets should contain only (bindingSets: _*)
    }
  }

  "QueryResultRecorder" should "produce a hash that's insensitive to result order" in {
    val (varNames, bindingSets) = randomResults(random.nextInt(200) + 2, random.nextInt(20) + 2)
    val tupleQueryResult: TupleQueryResult = toTupeQueryResult(varNames, bindingSets)
    val tupleQueryResult2: TupleQueryResult = toTupeQueryResult(scalaRandom.shuffle(varNames), scalaRandom.shuffle(bindingSets))
    (client.query(_: String)) expects "query 1" returns tupleQueryResult once()
    (client.query(_: String)) expects "query 2" returns tupleQueryResult2 once()
    val result = recorder.query("query 1")
    val result2 = recorder.query("query 2")
    result.reorderedHash shouldEqual result2.reorderedHash
  }

  "QueryResultRecorder" should "record failures" in {
    (client.query(_: String)) expects "query 1" throws new FatalException("Boom")
    val result = recorder.query("query 1")
    result.results shouldBe None
    result.resultSize shouldBe None
    result.exactHash shouldBe None
    result.reorderedHash shouldBe None
    result.success shouldBe false
    result.errorMessage.getOrElse("none") should include("Boom")
  }

  "QueryResultRecorder" should "be usable as a spark UDF" in {
    val inputType = StructType(Array(StructField(name = "query", dataType = DataTypes.StringType), StructField(name = "prov", dataType = DataTypes.StringType)))
    val inputDf = spark.createDataFrame(util.Arrays.asList(Row("query 1", "provenance 1"), Row("query 2", "provenance 2")), inputType)
    val (varNames, bindingSets) = randomResults(random.nextInt(200) + 2, random.nextInt(20) + 2)
    val tupleQueryResult: TupleQueryResult = toTupeQueryResult(varNames, bindingSets)

    (client.query(_: String)) expects "query 1" returns tupleQueryResult once()
    (client.query(_: String)) expects "query 2" throws new FatalException("boom") once()

    val resultDf = inputDf
      .select(recorder(inputDf("query")).alias("query_result"), inputDf("prov"))
      .select("query_result.*", "prov")
      .orderBy("prov")
    resultDf.schema shouldBe StructType(QueryResultRecorder.outputStruct.fields :+ StructField(name = "prov", dataType = DataTypes.StringType))
    val results = resultDf.collect()
    results should have size 2
    val (r1, r2) = (results(0), results(1))
    val (actualVarNames, actualBindingSets) = decodeResult(r1.getAs("results"))
    actualVarNames should contain only (varNames: _*)
    actualBindingSets should contain only (bindingSets distinct: _*)
    r1.getAs[Boolean]("success") shouldBe true
    Option(r1.getAs[String]("error_msg")) shouldBe None
    Option(r1.getAs[String]("exactHash")) should not be None
    Option(r1.getAs[String]("reorderedHash")) should not be None
    r1.getAs[Integer]("resultSize") shouldBe bindingSets.size

    r2.getAs[Boolean]("success") shouldBe false
    r2.getAs[String]("error_msg") should include("boom")
    Option(r2.getAs[Array[Byte]]("results")) shouldBe None
    Option(r2.getAs[String]("exactHash")) shouldBe None
    Option(r2.getAs[String]("reorderedHash")) shouldBe None
    Option(r2.getAs[Integer]("resultSize")) shouldBe None
  }

  "QueryResultRecorder" should "be serializable" in {
    val recorder = QueryResultRecorder.create("https://some.endpoint.unit-test.local", "unit test")
    recorder(col("test"))
    val deserRecorder = serDeser(recorder)
    deserRecorder should not be Nil
    deserRecorder(col("test")) should not be Nil
  }

  "QueryResultRecorder" should "collate string keys with tertiary strength" in {
    // Test that Noe͏̈l and Noël is "identical"
    // U+0065 : LATIN SMALL LETTER E
    // U+034F: COMBINING GRAPHEME JOINER [CGJ]
    // U+0308: COMBINING DIAERESIS
    // vs
    // U+00EB : LATIN SMALL LETTER E WITH DIAERESIS
    val (varNames, bindingSets1) = (List("res"), List(new ListBindingSet(List("res").asJava, valueFactory.createLiteral("Noe͏̈l"))))
    val bindingSets2 = List(new ListBindingSet(List("res").asJava, valueFactory.createLiteral("Noël")))
    val tupleQueryResult: TupleQueryResult = toTupeQueryResult(varNames, bindingSets1)
    val tupleQueryResult2: TupleQueryResult = toTupeQueryResult(varNames, bindingSets2)
    (client.query(_: String)) expects "query 1" returns tupleQueryResult once()
    (client.query(_: String)) expects "query 2" returns tupleQueryResult2 once()
    val result = recorder.query("query 1")
    val result2 = recorder.query("query 2")
    result.exactHash shouldEqual result2.exactHash
    result.reorderedHash shouldEqual result2.reorderedHash
  }

  def decodeResult(results: Iterable[Map[String, String]]): (List[String], List[BindingSet]) = {
    val varNames: List[String] = results.toList flatMap { _.keys } distinct
    val bindings: List[BindingSet] = results.toList map {s =>
      val keys = s.keys toList
      val values: List[Value] = keys map { s(_) } map { NTriplesUtil.parseValue(_, valueFactory) }

      new ListBindingSet(keys.asJava, values.asJava)
    }
    (varNames, bindings)
  }

  private def serDeser[T](ref: T): T = {
    val baos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(baos)
    os.writeObject(ref)
    os.close()

    new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray)).readObject().asInstanceOf[T]
  }

  private def toTupeQueryResult(varNames: List[String], bindingSets: List[BindingSet]) = {
    new TupleQueryResultImpl(util.Arrays.asList(varNames: _*), util.Arrays.asList(bindingSets: _*): lang.Iterable[BindingSet])
  }

  private def randomResults(rows: Int, columns: Int): (List[String], List[BindingSet]) = {
    val vars: List[String] = 1 to columns map {
      random.nextInt(20) + "_" + _
    } toList

    val bindingSets: List[BindingSet] = 1 to rows map { _ =>
      val bs = new MapBindingSet(vars.size)
      vars map {
        v => new BindingImpl(v, randomValue())
      } foreach {
        bs.addBinding
      }
      bs
    } toList

    (vars, bindingSets)
  }

  private val factories: Array[() => Value] = Array(
    () => valueFactory.createURI("uri:something" + random.nextInt(100)),
    () => valueFactory.createURI("uri:something#", "value_" + random.nextInt(100)),
    () => valueFactory.createBNode("bnode" + random.nextInt(100)),
    () => valueFactory.createLiteral(random.nextInt(1) == 1),
    () => valueFactory.createLiteral(random.nextDouble()),
    () => valueFactory.createLiteral(random.nextInt(255).toByte),
    () => valueFactory.createLiteral(random.nextInt()),
    () => valueFactory.createLiteral(random.nextLong()),
    () => valueFactory.createLiteral("random string " + random.nextInt(100)),
    () => valueFactory.createLiteral("random string " + random.nextInt(100), Array("it", "hi", "ar").apply(random.nextInt(3))),
    () => valueFactory.createLiteral(Instant.ofEpochMilli(random.nextLong()).toString, "xsd:dateTime")
  )

  private def randomValue(): Value = {
    factories(random.nextInt(factories.length))()
  }
}

object QueryResultRecorderTest {
  val seed: Long = Integer.parseInt(System.getProperty("RDF_SPARK_TOOLS_TEST_SEED", String.valueOf(System.currentTimeMillis() % 7919)))
}
