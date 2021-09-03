package org.wikidata.query.rdf.updater.config

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

class UpdaterConfig(args: Array[String]) extends BaseConfig()(BaseConfig.params(args)) {
  private val hostName: String = getStringParam("hostname")
  val jobName: String = getStringParam("job_name")
  val inputKafkaBrokers: String = getStringParam("brokers")
  val outputKafkaBrokers: String = params.get("output_brokers", inputKafkaBrokers)
  val outputTopic: String = getStringParam("output_topic")
  val outputPartition: Int = params.getInt("output_topic_partition")
  val entityNamespaces: Set[Long] = params.get("entity_namespaces", "").split(",").map(_.trim.toLong).toSet
  val mediaInfoEntityNamespaces: Set[Long] = params.get("mediainfo_entity_namespaces", "").split(",").map(_.trim.toLong).toSet
  val entityDataPath: String = params.get("wikibase_entitydata_path", Uris.DEFAULT_ENTITY_DATA_PATH)
  val useVersionedSerializers: Boolean = params.getBoolean("use_versioned_serializers", false)
  if (entityNamespaces.isEmpty && mediaInfoEntityNamespaces.isEmpty) {
    throw new IllegalArgumentException("entity_namespaces and/or mediainfo_entity_namespaces")
  }

  val generalConfig: UpdaterPipelineGeneralConfig = UpdaterPipelineGeneralConfig(
    hostname = hostName,
    jobName = jobName,
    entityNamespaces = entityNamespaces ++ mediaInfoEntityNamespaces,
    entityDataPath = entityDataPath,
    reorderingWindowLengthMs = params.getInt("reordering_window_length", 1 minute),

    generateDiffTimeout = params.getLong("generate_diff_timeout", 5.minutes.toMillis),
    wikibaseRepoThreadPoolSize = params.getInt("wikibase_repo_thread_pool_size", 30), // at most 60 concurrent requests to wikibase
    // T262020 and FLINK-11654 (might change to something more explicit on the KafkaProducer rather than reusing operator's name
    outputOperatorNameAndUuid = s"$outputTopic:$outputPartition",
    httpClientConfig = HttpClientConfig(
      httpRoutes = optionalStringArg("http_routes"),
      httpTimeout = optionalIntArg("http_timeout"),
      userAgent = params.get("user_agent", HttpClientUtils.WDQS_DEFAULT_UA)
    ),
    useVersionedSerializers = useVersionedSerializers
  )

  val inputEventStreamConfig: UpdaterPipelineInputEventStreamConfig = UpdaterPipelineInputEventStreamConfig(kafkaBrokers = inputKafkaBrokers,
    inputKafkaTopics = getInputKafkaTopics,
    consumerGroup = params.get("consumer_group", "wdqs_streaming_updater"),
    maxLateness = params.getInt("max_lateness", 1 minute),
    idleness = params.getInt("input_idleness", 1 minute),
    mediaInfoEntityNamespaces = params.get("mediainfo_entity_namespaces", "3,5,6")
      .split(',').map(_.toLong).toSet
  )

  val environmentConfig: UpdaterExecutionEnvironmentConfig = UpdaterExecutionEnvironmentConfig(checkpointDir = checkpointDir,
    checkpointInterval = params.getInt("checkpoint_interval", 3 minutes),
    checkpointTimeout = params.getInt("checkpoint_timeout", 10 minutes),
    minPauseBetweenCheckpoints = params.getInt("min_pause_between_checkpoints", 2 seconds),
    autoWMInterval = params.getInt("auto_wm_interval", 200 millis),
    checkpointingMode = if (params.getBoolean("exactly_once", true)) {
      CheckpointingMode.EXACTLY_ONCE
    } else {
      CheckpointingMode.AT_LEAST_ONCE
    },
    unalignedCheckpoints = params.getBoolean("unaligned_checkpoints", false),
    networkBufferTimeout = params.getInt("network_buffer_timeout", 100 millis),
    latencyTrackingInterval = optionalIntArg("latency_tracking_interval"),
    restartFailureRateDelay = Time.milliseconds(params.getInt("restart_failures_rate_delay", 10 seconds)),
    restartFailureRateInterval = Time.milliseconds(params.getInt("restart_failures_rate_interval", 30 minutes)),
    restartFailureRateMaxPerInternal = params.getInt("restart_failures_rate_max_per_interval", 2),
    parallelism = params.getInt("parallelism", 1)
  )

  val checkpointingMode: CheckpointingMode = if (params.getBoolean("exactly_once", true)) {
    CheckpointingMode.EXACTLY_ONCE
  } else {
    CheckpointingMode.AT_LEAST_ONCE
  }
  val outputStreamConfig: UpdaterPipelineOutputStreamConfig =
    UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = outputKafkaBrokers,
      topic = outputTopic,
      partition = outputPartition,
      checkpointingMode = checkpointingMode,
      eventStreamConfigEndpoint = params.get("event_stream_config_endpoint", WikimediaDefaults.EVENT_STREAM_CONFIG_URI),
      outputTopicPrefix = optionalStringArg("output_topic_prefix"),
      sideOutputsDomain = params.get("side_outputs_domain", hostName),
      sideOutputsKafkaBrokers = optionalStringArg("side_outputs_kafka_brokers"),
      lateEventOutputDir = optionalStringArg("late_events_dir"),
      failedEventOutputDir = optionalStringArg("failed_ops_dir"),
      spuriousEventOutputDir = optionalStringArg("spurious_events_dir"),
      schemaRepos = params.get(
        "schema_repositories",
        "https://schema.wikimedia.org/repositories/primary/jsonschema,https://schema.wikimedia.org/repositories/secondary/jsonschema"
      ).split(",")
        .map(_.trim)
        .toList
    )

  implicit def finiteDuration2Int(fd: FiniteDuration): Int = fd.toMillis.intValue
}

object UpdaterConfig {
  def apply(args: Array[String]): UpdaterConfig = new UpdaterConfig(args)
}

sealed case class UpdaterPipelineGeneralConfig(hostname: String,
                                               jobName: String,
                                               entityNamespaces: Set[Long],
                                               entityDataPath: String,
                                               reorderingWindowLengthMs: Int,
                                               generateDiffTimeout: Long,
                                               wikibaseRepoThreadPoolSize: Int,
                                               outputOperatorNameAndUuid: String,
                                               httpClientConfig: HttpClientConfig,
                                               useVersionedSerializers: Boolean
                                              )

sealed case class HttpClientConfig(
                                    httpRoutes: Option[String],
                                    httpTimeout: Option[Int],
                                    userAgent: String
                                  )

sealed case class UpdaterPipelineInputEventStreamConfig(kafkaBrokers: String,
                                                        consumerGroup: String,
                                                        inputKafkaTopics: InputKafkaTopics,
                                                        maxLateness: Int,
                                                        idleness: Int,
                                                        mediaInfoEntityNamespaces: Set[Long]
                                                       )

sealed case class UpdaterPipelineOutputStreamConfig(
                                                     kafkaBrokers: String,
                                                     topic: String,
                                                     partition: Int,
                                                     checkpointingMode: CheckpointingMode,
                                                     eventStreamConfigEndpoint: String = WikimediaDefaults.EVENT_STREAM_CONFIG_URI,
                                                     outputTopicPrefix: Option[String] = None,
                                                     sideOutputsDomain: String,
                                                     sideOutputsKafkaBrokers: Option[String],
                                                     lateEventOutputDir: Option[String],
                                                     spuriousEventOutputDir: Option[String],
                                                     failedEventOutputDir: Option[String],
                                                     schemaRepos: List[String]
                                                   )

sealed case class UpdaterExecutionEnvironmentConfig(checkpointDir: String,
                                                    checkpointInterval: Int,
                                                    checkpointTimeout: Int,
                                                    minPauseBetweenCheckpoints: Int,
                                                    autoWMInterval: Int,
                                                    checkpointingMode: CheckpointingMode,
                                                    unalignedCheckpoints: Boolean,
                                                    networkBufferTimeout: Int,
                                                    latencyTrackingInterval: Option[Int],
                                                    restartFailureRateDelay: Time,
                                                    restartFailureRateInterval: Time,
                                                    restartFailureRateMaxPerInternal: Int,
                                                    parallelism: Int
                                                   )
