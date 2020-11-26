package org.wikidata.query.rdf.spark

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

  def parse(inputStream: InputStream): List[Statement] = {
    val statements = new ListBuffer[Statement]()
    val handler = new RDFHandler {
      override def startRDF(): Unit = {}
      override def endRDF(): Unit = {}
      override def handleNamespace(prefix: String, uri: String): Unit = {}
      override def handleComment(comment: String): Unit = {}

      override def handleStatement(st: Statement): Unit = {
        statements += st
      }
    }
    val countListener = new EntityCountListener {
      override def entitiesProcessed(l: Long): Unit = {}
    }
    val mungingRdfHandler = new EntityMungingRdfHandler(urisScheme, this.munger, handler, countListener, quadGenerator)

    val parser = RDFParserSuppliers.defaultRdfParser().get(new NormalizingRdfHandler(mungingRdfHandler));
    parser.parse(new SequenceInputStream(new ByteArrayInputStream(makePrefixHeader().getBytes(StandardCharsets.UTF_8)), inputStream), urisScheme.root())
    statements.toList
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

  private def makePrefixHeader(): String = {
    namespaces map { case (k, v) => s"@prefix $k: $v ." } mkString "\n"
  }
}

object RdfChunkParser {
  // TODO: stop hardcoding prefixes, either generate from the hostname
  //  or read them from the dump
  private val prefixes = Map[String, String](
    "rdf" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "xsd" -> "<http://www.w3.org/2001/XMLSchema#>",
    "ontolex" -> "<http://www.w3.org/ns/lemon/ontolex#>",
    "dct" -> "<http://purl.org/dc/terms/>",
    "rdfs" -> "<http://www.w3.org/2000/01/rdf-schema#>",
    "owl" -> "<http://www.w3.org/2002/07/owl#>",
    "wikibase" -> "<http://wikiba.se/ontology#>",
    "skos" -> "<http://www.w3.org/2004/02/skos/core#>",
    "schema" -> "<http://schema.org/>",
    "cc" -> "<http://creativecommons.org/ns#>",
    "geo" -> "<http://www.opengis.net/ont/geosparql#>",
    "prov" -> "<http://www.w3.org/ns/prov#>",
    "v" -> "<http://www.wikidata.org/value/>",
    "wd" -> "<http://www.wikidata.org/entity/>",
    "data" -> "<https://www.wikidata.org/wiki/Special:EntityData/>",
    "s" -> "<http://www.wikidata.org/entity/statement/>",
    "ref" -> "<http://www.wikidata.org/reference/>",
    "wdt" -> "<http://www.wikidata.org/prop/direct/>",
    "wdtn" -> "<http://www.wikidata.org/prop/direct-normalized/>",
    "p" -> "<http://www.wikidata.org/prop/>",
    "ps" -> "<http://www.wikidata.org/prop/statement/>",
    "psv" -> "<http://www.wikidata.org/prop/statement/value/>",
    "psn" -> "<http://www.wikidata.org/prop/statement/value-normalized/>",
    "pq" -> "<http://www.wikidata.org/prop/qualifier/>",
    "pqv" -> "<http://www.wikidata.org/prop/qualifier/value/>",
    "pqn" -> "<http://www.wikidata.org/prop/qualifier/value-normalized/>",
    "pr" -> "<http://www.wikidata.org/prop/reference/>",
    "prv" -> "<http://www.wikidata.org/prop/reference/value/>",
    "prn" -> "<http://www.wikidata.org/prop/reference/value-normalized/>",
    "wdno" -> "<http://www.wikidata.org/prop/novalue/>"
  )

  def forWikidata(skolemize: Boolean = false): RdfChunkParser = {
    val urisScheme = UrisSchemeFactory.WIKIDATA
    new RdfChunkParser(urisScheme, buildMunger(skolemize, urisScheme), prefixes)
  }

  // TODO: stop hardcoding prefixes, either generate from the hostname
  //  or read them from the dump
  private val commons_prefixes = prefixes ++ Map[String, String](
"sdc" -> "<https://commons.wikimedia.org/entity/>",
    "sdcdata" -> "<https://commons.wikimedia.org/wiki/Special:EntityData/>",
    "sdcs" -> "<https://commons.wikimedia.org/entity/statement/>",
    "sdcref" -> "<https://commons.wikimedia.org/reference/>",
    "sdcv" -> "<https://commons.wikimedia.org/value/>",
    "sdct" -> "<https://commons.wikimedia.org/prop/direct/>",
    "sdctn" -> "<https://commons.wikimedia.org/prop/direct-normalized/>",
    "sdcp" -> "<https://commons.wikimedia.org/prop/>",
    "sdcps" -> "<https://commons.wikimedia.org/prop/statement/>",
    "sdcpsv" -> "<https://commons.wikimedia.org/prop/statement/value/>",
    "sdcpsn" -> "<https://commons.wikimedia.org/prop/statement/value-normalized/>",
    "sdcpq" -> "<https://commons.wikimedia.org/prop/qualifier/>",
    "sdcpqv" -> "<https://commons.wikimedia.org/prop/qualifier/value/>",
    "sdcpqn" -> "<https://commons.wikimedia.org/prop/qualifier/value-normalized/>",
    "sdcpr" -> "<https://commons.wikimedia.org/prop/reference/>",
    "sdcprv" -> "<https://commons.wikimedia.org/prop/reference/value/>",
    "sdcprn" -> "<https://commons.wikimedia.org/prop/reference/value-normalized/>",
    "sdcno" -> "<https://commons.wikimedia.org/prop/novalue/>"
  )

  def forCommons(skolemize: Boolean = false): RdfChunkParser = {
    val urisScheme = UrisSchemeFactory.fromConceptUris("http://www.wikidata.org/", "https://commons.wikimedia.org")
    new RdfChunkParser(urisScheme, buildMunger(skolemize, urisScheme), commons_prefixes)
  }

  def bySite(site: Site.Value, skolemize: Boolean): RdfChunkParser = {
    site match {
      case Site.wikidata => forWikidata(skolemize)
      case Site.commons => forCommons(skolemize)
    }
  }

  private def buildMunger(skolemize: Boolean, urisScheme: UrisScheme) = {
    val munger = Munger.builder(urisScheme).convertBNodesToSkolemIRIs(skolemize).build()
    // also hardcode format version for now
    munger.setFormatVersion("1.0.0")
    munger
  }
}
