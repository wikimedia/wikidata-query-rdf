package org.wikidata.query.rdf.updater.config

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import scala.language.{implicitConversions, postfixOps}
import scala.concurrent.duration._

import org.apache.flink.api.common.time.Time
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

class UpdaterConfig(args: Array[String]) extends BaseConfig()(BaseConfig.params(args)) {
  private val defaultEntityNs: String = Uris.DEFAULT_ENTITY_NAMESPACES.asScala.mkString(",")
  private val hostName: String = getStringParam("hostname")
  val jobName: String = getStringParam("job_name")
  val inputKafkaBrokers: String = getStringParam("brokers")
  val outputKafkaBrokers: String = params.get("output_brokers", inputKafkaBrokers)
  val outputTopic: String = getStringParam("output_topic")
  val outputPartition: Int = params.getInt("output_topic_partition")
  val entityNamespaces: Set[Long] = params.get("entity_namespaces", defaultEntityNs).split(",").map(_.trim.toLong).toSet

  val generalConfig: UpdaterPipelineGeneralConfig = UpdaterPipelineGeneralConfig(
    hostname = hostName,
    jobName = jobName,
    entityNamespaces = entityNamespaces,
    reorderingWindowLengthMs = params.getInt("reordering_window_length", 1 minute),

    generateDiffTimeout = params.getLong("generate_diff_timeout", 5.minutes.toMillis),
    wikibaseRepoThreadPoolSize = params.getInt("wikibase_repo_thread_pool_size", 30), // at most 60 concurrent requests to wikibase
    // T262020 and FLINK-11654 (might change to something more explicit on the KafkaProducer rather than reusing operator's name
    outputOperatorNameAndUuid = s"$outputTopic:$outputPartition"
  )

  val inputEventStreamConfig: UpdaterPipelineInputEventStreamConfig = UpdaterPipelineInputEventStreamConfig(kafkaBrokers = inputKafkaBrokers,
    revisionCreateTopicName = getStringParam("rev_create_topic"),
    pageDeleteTopicName = getStringParam("page_delete_topic"),
    pageUndeleteTopicName = getStringParam("page_undelete_topic"),
    suppressedDeleteTopicName = getStringParam("suppressed_delete_topic"),
    topicPrefixes = params.get("topic_prefixes", "").split(",").toList,
    consumerGroup = params.get("consumer_group", "wdqs_streaming_updater"),
    maxLateness = params.getInt("max_lateness", 1 minute),
    idleness = params.getInt("input_idleness", 1 minute)
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
      spuriousEventOutputDir = optionalStringArg("spurious_events_dir")
    )

  implicit def finiteDuration2Int(fd: FiniteDuration): Int = fd.toMillis.intValue

  private def optionalIntArg(paramName: String)(implicit params: ParameterTool): Option[Int] = {
    if (params.has(paramName)) {
      Some(params.getInt(paramName))
    } else {
      None
    }
  }

  private def optionalStringArg(paramName: String)(implicit params: ParameterTool): Option[String] = {
    if (params.has(paramName)) {
      Some(params.get(paramName))
    } else {
      None
    }
  }
}

object UpdaterConfig {
  def apply(args: Array[String]): UpdaterConfig = new UpdaterConfig(args)
}

sealed case class UpdaterPipelineGeneralConfig(hostname: String,
                                               jobName: String,
                                               entityNamespaces: Set[Long],
                                               reorderingWindowLengthMs: Int,
                                               generateDiffTimeout: Long,
                                               wikibaseRepoThreadPoolSize: Int,
                                               outputOperatorNameAndUuid: String
                                              )

sealed case class UpdaterPipelineInputEventStreamConfig(kafkaBrokers: String,
                                                        consumerGroup: String,
                                                        revisionCreateTopicName: String,
                                                        pageDeleteTopicName: String,
                                                        pageUndeleteTopicName: String,
                                                        suppressedDeleteTopicName: String,
                                                        topicPrefixes: List[String],
                                                        maxLateness: Int,
                                                        idleness: Int)

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
                                                     failedEventOutputDir: Option[String]
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
