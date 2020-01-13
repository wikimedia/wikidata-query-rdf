package org.wikidata.query.rdf.spark

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ListBuffer

import org.openrdf.model.Statement
import org.openrdf.rio.RDFHandler
import org.openrdf.rio.turtle.TurtleParser
import org.wikidata.query.rdf.common.uri.{UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.rdf.{EntityMungingRdfHandler, Munger, NormalizingRdfHandler}
import org.wikidata.query.rdf.tool.rdf.EntityMungingRdfHandler.EntityCountListener

/**
 * Parse a chunk of the wikibase rdf dump in turtle format
 * The InputStream must represent a valid portion of the dump with consistent entities.
 * Entity delimitation is any line of type:
 * data:QID a schema:Dataset ;
 * So any line starting with "data:" should mark the beginning of an entity
 */
class RdfChunkParser(urisScheme: UrisScheme, munger: Munger, namespaces: Map[String, String]) {
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
    val mungingRdfHandler = new EntityMungingRdfHandler(urisScheme, this.munger, handler, countListener)
    val parser = new TurtleParser()
    parser.setRDFHandler(new NormalizingRdfHandler(mungingRdfHandler))
    parser.parse(new SequenceInputStream(new ByteArrayInputStream(makePrefixHeader().getBytes(StandardCharsets.UTF_8)), inputStream), urisScheme.root())
    statements.toList
  }

  private def makePrefixHeader(): String = {
    namespaces map{case (k, v) => s"@prefix $k: $v ."} mkString "\n"
  }
}

object RdfChunkParser {
  def forWikidata(): RdfChunkParser = {
    // hardcode prefixes for now
    val prefixes = Map[String, String](
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
    val urisScheme = UrisSchemeFactory.WIKIDATA
    val munger = Munger.builder(urisScheme).build()
    // also hardcode format version for now
    munger.setFormatVersion("1.0.0")
    new RdfChunkParser(urisScheme, munger, prefixes)
  }
}
