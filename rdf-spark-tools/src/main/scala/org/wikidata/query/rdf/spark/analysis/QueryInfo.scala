package org.wikidata.query.rdf.spark.analysis

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.walker.Walker
import org.wikidata.query.rdf.spark.analysis.visitors.{AnalyzeOpVisitor, TripleInfo}

import scala.collection.mutable
import scala.util.Try

case class QueryInfo(
  queryReprinted: String,
  opList: Option[mutable.Buffer[String]],
  operators: Option[mutable.Map[String, Long]],
  prefixes: Option[mutable.Map[String, Long]],
  nodes: Option[mutable.Map[String, Long]],
  services: Option[mutable.Map[String, Long]],
  wikidataNames: Option[mutable.Map[String, Long]],
  expressions: Option[mutable.Map[String, Long]],
  paths: Option[mutable.Map[String, Long]],
  triples: Option[mutable.Buffer[TripleInfo]]
)

object QueryInfo {

  val prefixes = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX schema: <http://schema.org/>
    PREFIX cc: <http://creativecommons.org/ns#>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX v: <http://www.wikidata.org/value/>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX data: <https://www.wikidata.org/wiki/Special:EntityData/>
    PREFIX s: <http://www.wikidata.org/entity/statement/>
    PREFIX ref: <http://www.wikidata.org/reference/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
    PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
    PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
    PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
    PREFIX pr: <http://www.wikidata.org/prop/reference/>
    PREFIX prv: <http://www.wikidata.org/prop/reference/value/>
    PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>
    PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
    PREFIX bd: <http://www.bigdata.com/rdf#>
    PREFIX hint: <http://www.bigdata.com/queryHints#>
  """

  def apply(queryString: String): Option[QueryInfo] = Try{
    val queryWithNamespaces = QueryFactory.create(prefixes + queryString)
    val prefixMapping = queryWithNamespaces.getPrologue.getPrefixMapping
    val ast = Algebra.compile(queryWithNamespaces)
    val opAnalyzer = new AnalyzeOpVisitor(prefixMapping)
    Walker.walk(ast, opAnalyzer)

    // Hive complains with empty arrays and maps,
    // so they are converted to 'null' using Options.
    QueryInfo(
      //queryId,
      //queryString,
      ast.toString,
      Option(opAnalyzer.opList).filterNot(_.isEmpty),
      Option(opAnalyzer.opCount).filterNot(_.isEmpty),
      Option(opAnalyzer.nodeVisitor.prefixesCount).filterNot(_.isEmpty),
      Option(opAnalyzer.nodeVisitor.nodeCount).filterNot(_.isEmpty),
      Option(opAnalyzer.serviceVisitor.nodeCount).filterNot(_.isEmpty),
      Option(opAnalyzer.nodeVisitor.wdNodeCount).filterNot(_.isEmpty),
      Option(opAnalyzer.exprVisitor.exprVisited).filterNot(_.isEmpty),
      Option(opAnalyzer.pathVisitor.pathVisited).filterNot(_.isEmpty),
      Option(opAnalyzer.triples).filterNot(_.isEmpty)
    )

  }.toOption
}
