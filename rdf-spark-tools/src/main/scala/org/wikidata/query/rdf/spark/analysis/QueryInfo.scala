package org.wikidata.query.rdf.spark.analysis

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.walker.Walker
import org.wikidata.query.rdf.spark.analysis.visitors.{AnalyzeOpVisitor, TripleInfo}
import org.wikidata.query.rdf.common.uri._
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

  def makeURI(prefix: String, namespace: String): String = s"PREFIX $prefix: <$namespace>"

  val prefixDeclarations = List(
    makeURI("wikibase", Ontology.NAMESPACE),
    makeURI("skos", SKOS.NAMESPACE),
    makeURI("ontolex", Ontolex.NAMESPACE),
    makeURI("mediawiki", Mediawiki.NAMESPACE),
    makeURI("mwapi", Mediawiki.API),
    makeURI("geof", GeoSparql.FUNCTION_NAMESPACE),
    makeURI("geo", GeoSparql.NAMESPACE),
    makeURI("dct", Dct.NAMESPACE),
    makeURI("viaf", CommonValues.VIAF),
    makeURI("owl", OWL.NAMESPACE),
    makeURI("prov", Provenance.NAMESPACE),
    makeURI("rdf", RDF.NAMESPACE),
    makeURI("rdfs", RDFS.NAMESPACE),
    makeURI("schema", SchemaDotOrg.NAMESPACE),
    makeURI("gas", "http://www.bigdata.com/rdf/gas#"),
    makeURI("xsd", "http://www.w3.org/2001/XMLSchema#"),
    makeURI("cc", "http://creativecommons.org/ns#"),
    makeURI("bd", "http://www.bigdata.com/rdf#"),
    makeURI("hint", "http://www.bigdata.com/queryHints#"),
    makeURI("foaf", "http://xmlns.com/foaf/0.1/"),
    makeURI("fn", "http://www.w3.org/2005/xpath-functions#"),
    makeURI("dc", "http://purl.org/dc/elements/1.1/"),
    UrisSchemeFactory.WIKIDATA.prefixes(new java.lang.StringBuilder()).toString
  )

  val prefixes = prefixDeclarations.mkString("\n")

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
