package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RdfChunkParserUnitTest extends AnyFlatSpec with Matchers {
  "A RdfChunkParserUnitTest" should "extract some statements when given a chunk" in {
    val statements = RdfChunkParser.forWikidata().parse(getClass.getResourceAsStream("small_dump_chunk.ttl"))
    val uniqueContexts = statements.map({_.getContext.stringValue}).toSet
    uniqueContexts should contain allOf (
      "http://www.wikidata.org/entity/Q31",
      "http://www.wikidata.org/entity/Q8",
      "http://wikiba.se/ontology#Reference",
      "http://wikiba.se/ontology#Value",
      "http://wikiba.se/ontology#Dump")
  }
}
