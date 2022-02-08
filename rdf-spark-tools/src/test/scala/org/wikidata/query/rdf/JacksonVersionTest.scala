package org.wikidata.query.rdf

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.wikibase.{LatestRevisionResponse, WikibaseApiError}
import org.wikidata.query.rdf.tool.MapperUtils

/**
 * Simple test to make sure that the POJO required here can be deserialized
 * using the jackson version embarked by spark
 */
class JacksonVersionTest extends FlatSpec with Matchers {
  "Jackson" should "be able to deserialiaze lombok classes" in {
    val apiOutput = new LatestRevisionResponse(
      new LatestRevisionResponse.Query(
        Seq(
          new LatestRevisionResponse.Page(1L, "title", Seq(
              new LatestRevisionResponse.Revision(123L),
              new LatestRevisionResponse.Revision(124L)
            ).asJava, false
          ),
          new LatestRevisionResponse.Page(1L, "title",None.orNull, false)
        ).asJava), new WikibaseApiError("123", "error msg"));

    val bytes = MapperUtils.getObjectMapper.writeValueAsBytes(apiOutput)
    val apiOutputUnser = MapperUtils.getObjectMapper.readValue(bytes, classOf[LatestRevisionResponse])
    apiOutputUnser shouldBe apiOutput
  }
}
