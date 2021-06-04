package org.wikidata.query.rdf.updater

import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.util.InstantiationUtil
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

class OutputStreamsBuilderUnitTest extends FlatSpec with Matchers {

  "Output stream sinks" should "be serializable" in {
    val outputStreamConfig: UpdaterPipelineOutputStreamConfig =  UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = "test-broker",
      topic = "test-topic",
      partition = 0,
      checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
      eventStreamConfigEndpoint = WikimediaDefaults.EVENT_STREAM_CONFIG_URI,
      outputTopicPrefix = Some("test-prefix"),
      sideOutputsDomain = "test-host",
      sideOutputsKafkaBrokers = None,
      lateEventOutputDir = None,
      failedEventOutputDir = None,
      spuriousEventOutputDir = None,
      schemaRepos = List("https://schema.wikimedia.org/repositories/primary/jsonschema", "https://schema.wikimedia.org/repositories/secondary/jsonschema")
    )
    val outputStreams: OutputStreams = new OutputStreamsBuilder(outputStreamConfig, HttpClientConfig(None, None, "My agent")).build
    InstantiationUtil.serializeObject(outputStreams.failedOpsSink).length should not be 0
    InstantiationUtil.serializeObject(outputStreams.lateEventsSink).length should not be 0
    InstantiationUtil.serializeObject(outputStreams.spuriousEventsSink).length should not be 0
  }

  "Avro generic record wrapper" should "be serializable" in {
    InstantiationUtil.serializeObject(new GenericRecordWrapperFactory[InputEvent](InputEventEncoder.map,
      ParquetAvroWriters.forGenericRecord(InputEventEncoder.schema())))
    InstantiationUtil.serializeObject(new GenericRecordWrapperFactory[FailedOp](FailedOpEncoder.map,
      ParquetAvroWriters.forGenericRecord(FailedOpEncoder.schema())))
    InstantiationUtil.serializeObject(new GenericRecordWrapperFactory[IgnoredMutation](IgnoredMutationEncoder.map,
      ParquetAvroWriters.forGenericRecord(IgnoredMutationEncoder.schema())))
  }

}
