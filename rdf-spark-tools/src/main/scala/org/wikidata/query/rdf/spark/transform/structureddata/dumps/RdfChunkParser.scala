package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.nio.charset.StandardCharsets
import java.util.function.BiFunction

import scala.collection.mutable.ListBuffer

import org.openrdf.model.Statement
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.rio.RDFHandler
import org.wikidata.query.rdf.common.uri.{Ontology, UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.rdf._
import org.wikidata.query.rdf.tool.rdf.EntityMungingRdfHandler.EntityCountListener

/**
 * Parse a chunk of the wikibase rdf dump in turtle format
 * The InputStream must represent a valid portion of the dump with consistent entities.
 * Entity delimitation is any line of type:
 * data:QID a schema:Dataset ;
 * So any line starting with "data:" should mark the beginning of an entity
 */
class RdfChunkParser(urisScheme: UrisScheme, munger: Munger, namespaces: Map[String, String]) {
  val predicates = new NamespaceStatementPredicates(urisScheme)
  val valueFactory = new ValueFactoryImpl()

  /**
   * Plain parsing without any munging, only useful to parse some dump metadata triples.
   */
  def parseHeader(inputStream: InputStream): List[Statement] = {
    val statements = new ListBuffer[Statement]()
    val handler: RDFHandler = rdfHandler(statements)
    parse(inputStream, handler)
    statements.toList map headerQuadGenerator
  }

  def parseEntityChunk(inputStream: InputStream): List[Statement] = {
    val statements = new ListBuffer[Statement]()
    val handler: RDFHandler = rdfHandler(statements)
    val countListener = new EntityCountListener {
      override def entitiesProcessed(l: Long): Unit = {}
    }
    val mungingRdfHandler = new EntityMungingRdfHandler(urisScheme, this.munger, handler, countListener, quadGenerator)
    parse(inputStream, mungingRdfHandler)
    statements.toList
  }

  private def parse(inputStream: InputStream, handler: RDFHandler): Unit = {
    val parser = RDFParserSuppliers.defaultRdfParser().get(new NormalizingRdfHandler(handler))
    parser.parse(new SequenceInputStream(new ByteArrayInputStream(makePrefixHeader().getBytes(StandardCharsets.UTF_8)), inputStream), urisScheme.root())
  }

  private def rdfHandler(statements: ListBuffer[Statement]) = {
    val handler = new RDFHandler {
      override def startRDF(): Unit = {}

      override def endRDF(): Unit = {}

      override def handleNamespace(prefix: String, uri: String): Unit = {}

      override def handleComment(comment: String): Unit = {}

      override def handleStatement(st: Statement): Unit = {
        statements += st
      }
    }
    handler
  }

  /**
   * Generate quads for the purpose of attaching together all triples of a given entity.
   */
  private def quadGenerator: BiFunction[Statement, String, Statement] = new BiFunction[Statement, String, Statement] {
    def apply(st: Statement, entityId: String): Statement = {
      val contextURI = st match {
        // Dump statements are Bound to the dump file not a particular entity
        case s: Statement if StatementPredicates.dumpStatement(s) => Ontology.DUMP
        // Values & references cannot be bound to a single entity, let's put them in their rdfs:type for now, this does
        // not make much sense but preferable as assigning them to random entity.
        case s: Statement if predicates.subjectInReferenceNS(s) => Ontology.REFERENCE
        case s: Statement if predicates.subjectInValueNS(s) => Ontology.VALUE
        case _: Any => urisScheme.entityIdToURI(entityId)
      }
      valueFactory.createStatement(st.getSubject, st.getPredicate, st.getObject, valueFactory.createURI(contextURI))
    }
  }

  private def headerQuadGenerator: Function[Statement, Statement] = new Function[Statement, Statement] {
    def apply(st: Statement): Statement = {
      val contextURI = st match {
        // Dump statements are Bound to the dump file not a particular entity
        case s: Statement if StatementPredicates.dumpStatement(s) => Ontology.DUMP
        case _: Any => throw new IllegalArgumentException(s"Received $st but expected ")
      }
      valueFactory.createStatement(st.getSubject, st.getPredicate, st.getObject, valueFactory.createURI(contextURI))
    }
  }

  private def makePrefixHeader(): String = {
    namespaces map { case (k, v) => s"@prefix $k: <$v> ." } mkString "\n"
  }
}

object RdfChunkParser {
  def forWikidata(prefixes: Map[String, String], skolemize: Boolean = false): RdfChunkParser = {
    val urisScheme = UrisSchemeFactory.WIKIDATA
    new RdfChunkParser(urisScheme, buildMunger(skolemize, urisScheme), prefixes)
  }

  def forCommons(prefixes: Map[String, String], skolemize: Boolean = false): RdfChunkParser = {
    val urisScheme = UrisSchemeFactory.fromConceptUris("http://www.wikidata.org/", "https://commons.wikimedia.org")
    new RdfChunkParser(urisScheme, buildMunger(skolemize, urisScheme), prefixes)
  }

  def bySite(site: Site.Value, prefixes: Map[String, String], skolemize: Boolean): RdfChunkParser = {
    site match {
      case Site.wikidata => forWikidata(prefixes, skolemize)
      case Site.commons => forCommons(prefixes, skolemize)
    }
  }

  private def buildMunger(skolemize: Boolean, urisScheme: UrisScheme) = {
    val munger = Munger.builder(urisScheme).convertBNodesToSkolemIRIs(skolemize).build()
    // also hardcode format version for now
    munger.setFormatVersion("1.0.0")
    munger
  }
}
