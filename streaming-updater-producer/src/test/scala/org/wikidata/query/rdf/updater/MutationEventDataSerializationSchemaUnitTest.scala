package org.wikidata.query.rdf.updater

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.MapperUtils
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}

import java.time.Instant

class MutationEventDataSerializationSchemaUnitTest extends FlatSpec with Matchers with MockFactory {
  private val streamToTopicMap = Map.apply("stream1" -> "topic1", "stream2" -> "topic2")
  private val clock = () => Instant.EPOCH

  "MutationEventDataSerializationSchema" should "create a proper kafka record" in {
    val schema = new MutationEventDataSerializationSchema(streamToTopicMap, 0)
    val sinkContext = mock[KafkaRecordSerializationSchema.KafkaSinkContext]
    val input = buildChunk("stream1")
    val record = schema.serialize(input, sinkContext, 0)
    record.partition() shouldBe 0
    record.topic() shouldBe "topic1"
    Option(record.key()) shouldEqual None
    parseRecord(record.value()) shouldEqual input.data
  }

  private def parseRecord(bytes: Array[Byte]) = {
    MapperUtils.getObjectMapper.readValue(bytes, classOf[MutationEventDataV2])
  }

  "MutationEventDataSerializationSchema" should "fail if provided an unknown stream" in {
    val schema = new MutationEventDataSerializationSchema(streamToTopicMap, 0)
    val sinkContext = mock[KafkaRecordSerializationSchema.KafkaSinkContext]
    assertThrows[IllegalArgumentException](schema.serialize(buildChunk("unknown"), sinkContext, 0))
  }

  private def buildChunk(stream: String): MutationDataChunk = {
    MutationDataChunk(
      Diff("Q1", Instant.EPOCH, 2, 1, Instant.EPOCH,
        new EventInfo(
          new EventsMeta(Instant.EPOCH, "my-original-id", "my-domain", "source-stream", "my-request-id"),
          "source-schema"
        )
      ),
      new DiffEventDataV2(
        new EventsMeta(Instant.EPOCH, "my-id", "my-domain", stream, "my-request-id"),
         "Q1", 0, Instant.EPOCH, 0, 1,
        MutationEventData.DIFF_OPERATION,
        new RDFDataChunk("", ""),
        new RDFDataChunk("", ""),
        new RDFDataChunk("", ""),
        new RDFDataChunk("", "")))
  }
}
