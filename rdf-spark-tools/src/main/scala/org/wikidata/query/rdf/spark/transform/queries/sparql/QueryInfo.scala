package org.wikidata.query.rdf.spark.transform.queries.sparql

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.walker.Walker
import org.wikidata.query.rdf.spark.transform.queries.sparql.visitors.{AnalyzeOpVisitor, TripleInfo}
import org.wikidata.query.rdf.spark.utils.PrefixDeclarations

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

  val prefixes = PrefixDeclarations.getPrefixDeclarations

  def apply(queryString: String): Option[QueryInfo] = Try {
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
