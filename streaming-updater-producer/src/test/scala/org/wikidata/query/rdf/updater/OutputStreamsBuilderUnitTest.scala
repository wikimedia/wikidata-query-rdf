package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.util.InstantiationUtil
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

import scala.collection.immutable

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
      schemaRepos = List("https://schema.wikimedia.org/repositories/primary/jsonschema", "https://schema.wikimedia.org/repositories/secondary/jsonschema"),
      ignoreFailuresAfterTransactionTimeout = false,
      produceSideOutputs = true,
      emitterId = Some("id"),
      immutable.Map("main" -> "topic-main", "scholarly" -> "topic-scholarly"),
      Map.apply("someoption" -> "somevalue")
    )
    val outputStreams: OutputStreams = new OutputStreamsBuilder(outputStreamConfig, HttpClientConfig(None, None, "My agent"), List("main", "scholarly")).build
    InstantiationUtil.serializeObject(outputStreams.failedOpsSink.get.sink).length should not be 0
    InstantiationUtil.serializeObject(outputStreams.lateEventsSink.get.sink).length should not be 0
    InstantiationUtil.serializeObject(outputStreams.spuriousEventsSink.get.sink).length should not be 0
  }
}
