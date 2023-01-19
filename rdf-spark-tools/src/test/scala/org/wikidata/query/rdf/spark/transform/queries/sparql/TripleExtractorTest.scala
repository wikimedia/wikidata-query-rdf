package org.wikidata.query.rdf.spark.transform.queries.sparql

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.spark.transform.queries.sparql.visitors.{NodeInfo, TripleInfo}

class TripleExtractorTest extends AnyFlatSpec with Matchers {

  //Load SPARQL queries that will be successfully processed
  val successSparqls: Array[String] = TripleExtractorTest.getQueries("SuccessfulSparqlQueries.txt")
  val successTriples = TripleExtractorTest.getTriples(successSparqls)

  //Load SPARQL queries that will fail to be processed
  val failSparqls: Array[String] = TripleExtractorTest.getQueries("FailedSparqlQueries.txt")
  val failTriples = TripleExtractorTest.getTriples(failSparqls)

  //Test demo queries
  val testQueries = Array(
    """
    SELECT *
    {
      ?s wdt:P31/wdt:P279 <_:bn>;
         skos:altLabel "alias"@en.
    }""",
    """
    #Twisted Cat Query
    SELECT ?item
    WHERE
    {
      ?item wdt:P31 <_:0>.
      <_:0> ^wdt:P31 <_:1>.
      <_:1> wdt:P127 wd:Q67320820.
    }
    """
  )
  val testTriples = TripleExtractorTest.getTriples(testQueries)

  "The failed Queries" should " be empty" in {
    failTriples.foreach{ triples =>
      triples shouldBe empty
    }
  }

  "The successfully extracted triples" should "not be empty" in {
    successTriples.foreach{ triples =>
      triples should not be empty
    }
  }
  it should "be a mutable buffer (Array) of triples" in {
    successTriples.foreach{ triples =>
      // Space before left bracket required here as per syntax
      // scalastyle:off no.whitespace.before.left.bracket
      triples shouldBe a [mutable.Buffer[_]]
      // scalastyle:on no.whitespace.before.left.bracket
    }
  }

  it should "identify Variable nodes" in {
    testTriples(0)(0).subjectNode shouldBe NodeInfo("NODE_VAR", "s")
    testTriples(1)(0).subjectNode shouldBe NodeInfo("NODE_VAR", "item")
  }

  it should "identify URI nodes" in {
    testTriples(0)(1).predicateNode shouldBe NodeInfo("NODE_URI", "skos:altLabel")
    testTriples(1)(0).predicateNode shouldBe NodeInfo("NODE_URI", "wdt:P31")
  }

  it should "identify Literal nodes" in {
    testTriples(0)(1).objectNode shouldBe NodeInfo("NODE_LITERAL", "alias@en")
  }

  it should "identify Paths" in {
    testTriples(0)(0).predicateNode shouldBe NodeInfo("PATH", "<http://www.wikidata.org/prop/direct/P31>/<http://www.wikidata.org/prop/direct/P279>")
    testTriples(1)(1).predicateNode shouldBe NodeInfo("PATH", "^<http://www.wikidata.org/prop/direct/P31>")
  }

  it should "identify Blank Nodes" in {
    testTriples(0)(0).objectNode shouldBe NodeInfo("NODE_BLANK", "bn")
    testTriples(1)(0).objectNode shouldBe NodeInfo("NODE_BLANK", "0")
    testTriples(1)(1).objectNode shouldBe NodeInfo("NODE_BLANK", "1")
  }

}

object TripleExtractorTest{

  def getTriples(queries: Array[String]): Array[Seq[TripleInfo]] ={
    queries.map(QueryInfo(_).map(_.triples.getOrElse(Seq.empty)).getOrElse(Seq.empty))
  }

  def getQueries(filename: String): Array[String] = {
    val url = getClass.getResource(filename)
    val source: BufferedSource = Source.fromURL(url)
    val queries: Array[String] = source.mkString.trim.split("##SAMPLEQUERY##")
    source.close()
    queries
  }
}
