package org.wikidata.query.rdf.spark

import org.scalatest.FlatSpec

class RdfChunkParserUnitTest extends FlatSpec {
  "A RdfChunkParserUnitTest" should "extract some statements when given a chunk" in {
    val statements = RdfChunkParser.forWikidata().parse(getClass.getResourceAsStream("small_dump_chunk.ttl"))
    assert(statements.length == 20)
  }
}
