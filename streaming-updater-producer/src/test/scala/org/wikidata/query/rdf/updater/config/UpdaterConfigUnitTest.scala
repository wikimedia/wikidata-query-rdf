package org.wikidata.query.rdf.updater.config

import org.apache.commons.io.IOUtils
import org.apache.flink.streaming.api.CheckpointingMode
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.tool.subgraph.SubgraphDefinitionsParser
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

import java.nio.file.Files
import java.time.Instant
import scala.concurrent.duration.DurationInt

class UpdaterConfigUnitTest extends FlatSpec with Matchers {
  private val baseConfig = Array(
      "--job_name", "my job",
      "--checkpoint_dir", "fs://my_checkpoint",
      "--brokers", "broker1,broker2",
      "--output_topic", "my.output-topic",
      "--output_topic_partition", "1",
      "--entity_namespaces", "0,120",
      "--rev_create_topic", "mediawiki.revision-create",
      "--page_delete_topic", "mediawiki.page-delete",
      "--suppressed_delete_topic", "mediawiki.page-suppress",
      "--page_undelete_topic", "mediawiki.page-undelete",
      "--consumer_group", "my_consumer_group"
    )

  "UpdaterConfig" should "build a config when passing minimal arguments" in {
    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata"
    ))

    config.entityNamespaces should contain only (0, 120)
    config.mediaInfoEntityNamespaces shouldBe empty
    config.inputKafkaBrokers shouldBe "broker1,broker2"
    config.outputPartition shouldBe 1
    config.outputTopic shouldBe "my.output-topic"
    config.checkpointingMode shouldBe CheckpointingMode.EXACTLY_ONCE
    config.checkpointDir shouldBe "fs://my_checkpoint"

    config.environmentConfig.checkpointDir shouldBe "fs://my_checkpoint"
    config.environmentConfig.checkpointingMode shouldBe CheckpointingMode.EXACTLY_ONCE
    config.environmentConfig.parallelism shouldBe 1

    config.subgraphDefinition shouldBe None

    config.schemaBaseUris shouldBe List(
      "https://schema.wikimedia.org/repositories/primary/jsonschema",
      "https://schema.wikimedia.org/repositories/secondary/jsonschema"
    )
    config.streamConfigUri shouldBe WikimediaDefaults.EVENT_STREAM_CONFIG_URI

    config.inputEventStreamConfig.mediaInfoEntityNamespaces shouldBe empty
    config.inputEventStreamConfig.consumerGroup shouldBe "my_consumer_group"
    config.inputEventStreamConfig.kafkaBrokers shouldBe "broker1,broker2"
    config.inputEventStreamConfig.idleness shouldBe 60000
    config.inputEventStreamConfig.maxLateness shouldBe 60000
    config.inputEventStreamConfig.inputStreams shouldBe Left(InputKafkaTopics(
      revisionCreateTopicName = "mediawiki.revision-create",
      pageDeleteTopicName = "mediawiki.page-delete",
      pageUndeleteTopicName = "mediawiki.page-undelete",
      suppressedDeleteTopicName = "mediawiki.page-suppress",
      reconciliationTopicName = None,
      topicPrefixes = List("")
    ))
    config.inputEventStreamConfig.consumerProperties shouldBe empty

    config.generalConfig.jobName shouldBe "my job"
    config.generalConfig.hostname shouldBe "my.wikidata.org"
    config.generalConfig.entityDataPath shouldBe WikibaseRepository.Uris.DEFAULT_ENTITY_DATA_PATH
    config.generalConfig.entityNamespaces should contain only(0, 120)
    config.generalConfig.generateDiffTimeout shouldBe 300000
    config.generalConfig.reorderingWindowLengthMs shouldBe 60000
    config.generalConfig.wikibaseRepoThreadPoolSize shouldBe 10
    config.generalConfig.urisScheme.entityData() shouldBe "http://my.wikidata.org/wiki/Special:EntityData/"
    config.generalConfig.urisScheme.entityIdToURI("Q123") shouldBe "http://my.wikidata.org/entity/Q123"
    config.generalConfig.acceptableMediawikiLag shouldBe 10.seconds
    config.generalConfig.outputMutationSchemaVersion shouldBe "v1"

    config.generalConfig.httpClientConfig.httpRoutes shouldBe None
    config.generalConfig.httpClientConfig.httpTimeout shouldBe None
    config.generalConfig.httpClientConfig.userAgent shouldBe HttpClientUtils.WDQS_DEFAULT_UA

    config.outputStreamConfig.ignoreFailuresAfterTransactionTimeout shouldBe false
    config.outputStreamConfig.subgraphKafkaTopics shouldBe empty
    config.outputStreamConfig.producerProperties shouldBe Map.apply(
      "batch.size" -> "250000",
      "compression.type" -> "snappy",
      "linger.ms" -> "2000"
    )
    config.outputStreamConfig.useEventStreamsApi shouldBe false
  }

  "UpdaterConfig" should "build a config suited for commons with wikidata federation" in {
    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my-commons.wikimedia.org",
      "--mediainfo_entity_namespaces", "6",
      "--uris_scheme", "commons",
      "--wikidata_concept_uri", "https://my.wikidata.org"
    ))

    config.generalConfig.entityNamespaces should contain only(0, 120, 6)
    config.entityNamespaces should contain only(0, 120)
    config.mediaInfoEntityNamespaces should contain only(6)
    config.inputEventStreamConfig.mediaInfoEntityNamespaces should contain only(6)
    config.generalConfig.urisScheme.entityData() shouldBe "https://my-commons.wikimedia.org/wiki/Special:EntityData/"
    config.generalConfig.urisScheme.entityIdToURI("Q123") shouldBe "https://my.wikidata.org/entity/Q123"
    config.generalConfig.urisScheme.entityIdToURI("M123") shouldBe "https://my-commons.wikimedia.org/entity/M123"
  }

  "UpdaterConfig" should "build a config with a filtered reconciliation input topic" in {
    val configWithFilteredTopic = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--reconciliation_topic", "rdf-streaming-updater.reconciliation[source_tag@codfw]"
    ))

    configWithFilteredTopic.inputEventStreamConfig.inputStreams match {
      case Left(m) => m.reconciliationTopicName shouldBe Some(FilteredReconciliationTopic(
        topic = "rdf-streaming-updater.reconciliation",
        source = Some("source_tag@codfw")
      ))
      case _ => fail("expected left value")
    }

    val configWithUnfilteredTopic = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--reconciliation_topic", "rdf-streaming-updater.reconciliation"
    ))

    configWithUnfilteredTopic.inputEventStreamConfig.inputStreams match {
      case Left(m) => m.reconciliationTopicName shouldBe Some(FilteredReconciliationTopic(
        topic = "rdf-streaming-updater.reconciliation",
        source = None
      ))
      case _ => fail("expected left value")
    }
  }

  "UpdaterConfig" should "fail when given improper parallelism max concurrency settings" in {
    val caught = intercept[IllegalArgumentException] {
      UpdaterConfig(baseConfig ++ Array(
        "--hostname", "my.wikidata.org",
        "--mediawiki_max_concurrent_requests", "2",
        "--parallelism", "4"
      ))
    }
    assert(caught.getMessage == "The expected concurrency limits of 2 cannot be achieved with a parallelism of 4, " +
      "please set --parallelism to at most 2")
  }

  "UpdaterConfig" should "support setting a subgraph definition and topics for subgraphs" in {
    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--subgraph_definitions", "wdqs-subgraph-definitions-v2",
      "--subgraph_kafka_topics.rdf-streaming-updater.mutation-main", "topic-main",
      "--subgraph_kafka_topics.rdf-streaming-updater.mutation-scholarly", "topic-scholarly"))
    val expectedDefinitions = SubgraphDefinitionsParser.parseYaml(
      classOf[SubgraphDefinitionsParser].getResourceAsStream(s"/wdqs-subgraph-definitions-v2.yaml"))
    config.outputStreamConfig.subgraphKafkaTopics should contain theSameElementsAs Map(
      "rdf-streaming-updater.mutation-main" -> "topic-main",
      "rdf-streaming-updater.mutation-scholarly" -> "topic-scholarly"
    )
    config.subgraphDefinition shouldEqual Some(expectedDefinitions)
  }

  "UpdaterConfig" should "support loading a subgraph definition from a file" in {
    val file = Files.createTempFile(this.getClass.getSimpleName, "subgraph-def.yaml")
    file.toFile.deleteOnExit()
    IOUtils.toByteArray(classOf[SubgraphDefinitionsParser].getResource("/wdqs-subgraph-definitions-v2.yaml"))
    classOf[SubgraphDefinitionsParser].getResourceAsStream(s"/wdqs-subgraph-definitions-v2.yaml")
    Files.write(file, IOUtils.toByteArray(classOf[SubgraphDefinitionsParser].getResource("/wdqs-subgraph-definitions-v2.yaml")))

    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--subgraph_definitions", file.toUri.toString,
      "--subgraph_kafka_topics.rdf-streaming-updater.mutation-main", "topic-main",
      "--subgraph_kafka_topics.rdf-streaming-updater.mutation-scholarly", "topic-scholarly"))
    val expectedDefinitions = SubgraphDefinitionsParser.parseYaml(
      classOf[SubgraphDefinitionsParser].getResourceAsStream(s"/wdqs-subgraph-definitions-v2.yaml"))
    config.outputStreamConfig.subgraphKafkaTopics should contain theSameElementsAs Map(
      "rdf-streaming-updater.mutation-main" -> "topic-main",
      "rdf-streaming-updater.mutation-scholarly" -> "topic-scholarly"
    )
    config.subgraphDefinition shouldEqual Some(expectedDefinitions)
  }

  "UpdaterConfig" should "support pass kafka producer/consumer options" in {
    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--kafka_producer_config.linger.ms", "4000",
      "--kafka_producer_config.batch.size", "400000",
      "--kafka_producer_config.compression.type", "zstd",
      "--kafka_producer_config.security.protocol", "SSL",
      "--kafka_consumer_config.security.protocol", "SSL"
    ))
    config.outputStreamConfig.producerProperties shouldBe Map(
      "linger.ms" -> "4000",
      "batch.size" -> "400000",
      "compression.type" -> "zstd",
      "security.protocol" -> "SSL"
    )
    config.inputEventStreamConfig.consumerProperties shouldBe Map(
      "security.protocol" -> "SSL"
    )
  }

  "UpdaterConfig" should "allow setting EventPlatform config services" in {
    var config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--schema_base_uris", "https://myrepo.unittest.local/one,https://myrepo.unittest.local/one",
      "--stream_config_uri", "https://mystreamconfig.unittest.local/"
    ))
    config.schemaBaseUris shouldBe List("https://myrepo.unittest.local/one", "https://myrepo.unittest.local/one")
    config.streamConfigUri shouldBe "https://mystreamconfig.unittest.local/"

    config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--schema_repositories", "https://myrepo.unittest.local/one,https://myrepo.unittest.local/one",
      "--event_stream_config_endpoint", "https://mystreamconfig.unittest.local/"
    ))
    config.schemaBaseUris shouldBe List("https://myrepo.unittest.local/one", "https://myrepo.unittest.local/one")
    config.streamConfigUri shouldBe "https://mystreamconfig.unittest.local/"
  }

  "UpdaterConfig" should "support passing another output schema version" in {
    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--output_mutation_schema_version", "v2"
    ))
    config.generalConfig.outputMutationSchemaVersion shouldBe "v2"
  }

  "UpdaterConfig" should "support configuring streams with newer event streams api" in {
    val config = UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--use_event_streams_api", "true",
      "--output_mutation_schema_version", "v2",
      "--page_change_stream", "mediawiki.page_change",
      "--page_change_content_models", "content-model1, content-model2,,",
      "--reconciliation_stream", "reconcile_stream[source_tag]",
      "--kafka_topics_start_timestamp", "2024-01-01T01:01:01Z",
      "--main_output_stream", "main-output-stream"
    ))
    config.inputEventStreamConfig.inputStreams shouldBe Right(InputStreams(
      pageChangeStream = "mediawiki.page_change",
      reconciliationStream = Some(FilteredReconciliationStream("reconcile_stream", Some("source_tag"))),
      Some(Instant.parse("2024-01-01T01:01:01Z")),
      contentModels = Set("content-model1","content-model2")
    ))

    config.outputStreamConfig.useEventStreamsApi shouldBe true
    config.outputStreamConfig.mainStream shouldBe "main-output-stream"
  }

  "UpdaterConfig" should "fail when using newer event streams api and not running v2 output schema" in {
    the[IllegalArgumentException] thrownBy UpdaterConfig(baseConfig ++ Array(
        "--hostname", "my.wikidata.org",
        "--uris_scheme", "wikidata",
        "--use_event_streams_api", "true",
        "--page_change_stream", "mediawiki.page_change",
        "--page_change_content_models", "content-model1, content-model2,,",
        "--main_output_stream main-output-stream",
        "--reconciliation_stream", "reconcile_stream[source_tag]"
    )) should have message "Cannot use use_event_streams_api = true without setting output_mutation_schema_version = v2"
  }

  "UpdaterConfig" should "support fail when using newer event streams api and not providing the main output stream" in {
    the[IllegalArgumentException] thrownBy UpdaterConfig(baseConfig ++ Array(
      "--hostname", "my.wikidata.org",
      "--uris_scheme", "wikidata",
      "--use_event_streams_api", "true",
      "--page_change_stream", "mediawiki.page_change",
      "--page_change_content_models", "content-model1, content-model2,,",
      "--reconciliation_stream", "reconcile_stream[source_tag]",
      "--output_mutation_schema_version", "v2"
    )) should have message "missing param main_output_stream"
  }
}
