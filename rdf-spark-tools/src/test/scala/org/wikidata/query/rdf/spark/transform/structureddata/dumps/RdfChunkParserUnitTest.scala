package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.commons.io.IOUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

class RdfChunkParserUnitTest extends AnyFlatSpec with Matchers {
  "A RdfChunkParser" should "extract some statements when given a chunk" in {
    val statements = RdfChunkParser.forWikidata(Map.empty).parseEntityChunk(getClass.getResourceAsStream("small_dump_chunk.ttl"))
    val uniqueContexts = statements.map({_.getContext.stringValue}).toSet
    uniqueContexts should contain allOf (
      "http://www.wikidata.org/entity/Q31",
      "http://www.wikidata.org/entity/Q8",
      "http://wikiba.se/ontology#Reference",
      "http://wikiba.se/ontology#Value",
      "http://wikiba.se/ontology#Dump")
  }

  "A RdfChunkParser" should "extract some dump metadata statements when given a header" in {
    val dump = IOUtils.toString(getClass.getResourceAsStream("small_dump_chunk.ttl"), StandardCharsets.UTF_8)
    val header = Pattern.compile("^data:", Pattern.MULTILINE).split(dump)(0)
    val statements = RdfChunkParser.forWikidata(Map.empty).parseHeader(new ByteArrayInputStream(header.getBytes(StandardCharsets.UTF_8)))
    val uniqueContexts = statements.map({_.getContext.stringValue}).toSet
    uniqueContexts should contain only  "http://wikiba.se/ontology#Dump"
  }
}
