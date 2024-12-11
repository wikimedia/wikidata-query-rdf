package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.util.InstantiationUtil
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

import java.time.Clock
import scala.collection.immutable

object OutputStreamsBuilderUnitTestFixtures {
  val eventPlatformFactory = new EventPlatformFactory(this.getClass.getResource("/OutputStreamsBuilderUnitTest-stream-config.json").toString,
    List("https://schema.wikimedia.org/repositories/primary/jsonschema",
      "https://schema.wikimedia.org/repositories/secondary/jsonschema",
      this.getClass.getResource("/schema_repo").toString
    ),
    HttpClientConfig(None, None, "ua"),
    Clock.systemUTC()
  )

}

class OutputStreamsBuilderUnitTest extends FlatSpec with Matchers {
  "Output stream sinks" should "be serializable" in {
    val outputStreamConfig: UpdaterPipelineOutputStreamConfig =  UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = "test-broker",
      topic = "test-topic",
      partition = 0,
      checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
      outputTopicPrefix = Some("test-prefix"),
      sideOutputsDomain = "test-host",
      sideOutputsKafkaBrokers = None,
      ignoreFailuresAfterTransactionTimeout = false,
      produceSideOutputs = true,
      emitterId = Some("id"),
      immutable.Map("main" -> "topic-main", "scholarly" -> "topic-scholarly"),
      Map.apply("someoption" -> "somevalue"),
      mainStream = "rdf-streaming-updater.mutation",
      useEventStreamsApi = true,
      streamVersionSuffix = None
    )
    val eventPlatformFactory = new EventPlatformFactory(WikimediaDefaults.EVENT_STREAM_CONFIG_URI,
      List("https://schema.wikimedia.org/repositories/primary/jsonschema",
        "https://schema.wikimedia.org/repositories/secondary/jsonschema"),
      HttpClientConfig(None, None, "ua"),
      Clock.systemUTC()
    )
    val outputStreams: OutputStreams = new OutputStreamsBuilder(outputStreamConfig, eventPlatformFactory, 1).build
    InstantiationUtil.serializeObject(outputStreams.failedOpsSink.get).length should not be 0
    InstantiationUtil.serializeObject(outputStreams.lateEventsSink.get).length should not be 0
    InstantiationUtil.serializeObject(outputStreams.spuriousEventsSink.get).length should not be 0
  }
}
