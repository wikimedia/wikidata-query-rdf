package org.wikidata.query.rdf.updater.config

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode


import scala.language.{implicitConversions, postfixOps}
import scala.concurrent.duration._

class UpdaterConfig(args: Array[String]) extends BaseConfig()(BaseConfig.params(args)) {
  private val hostName: String = getStringParam("hostname")

  val inputKafkaBrokers: String = getStringParam("brokers")
  val outputKafkaBrokers: String = params.get("output_brokers", inputKafkaBrokers)
  val outputTopic: String = getStringParam("output_topic")
  val outputPartition: Int = params.getInt("output_topic_partition")

  val generalConfig: UpdaterPipelineGeneralConfig = UpdaterPipelineGeneralConfig(
    hostname = hostName,
    reorderingWindowLengthMs = params.getInt("reordering_window_length", 1 minute),
    reorderingOpParallelism = optionalIntArg("reordering_parallelism"),
    decideMutationOpParallelism = optionalIntArg("decide_mut_op_parallelism"),
    generateDiffParallelism = params.getInt("generate_diff_parallelism", 2),
    generateDiffTimeout = params.getLong("generate_diff_timeout", 5.minutes.toMillis),
    wikibaseRepoThreadPoolSize = params.getInt("wikibase_repo_thread_pool_size", 30), // at most 60 concurrent requests to wikibase
    // T262020 and FLINK-11654 (might change to something more explicit on the KafkaProducer rather than reusing operator's name
    outputOperatorNameAndUuid = s"$outputTopic:$outputPartition"
  )

  val InputEventStreamConfig: UpdaterPipelineInputEventStreamConfig = UpdaterPipelineInputEventStreamConfig(kafkaBrokers = inputKafkaBrokers,
    revisionCreateTopicName = getStringParam("rev_create_topic"),
    pageDeleteTopicName = getStringParam("page_delete_topic"),
    pageUndeleteTopicName = getStringParam("page_undelete_topic"),
    suppressedDeleteTopicName = getStringParam("suppressed_delete_topic"),
    topicPrefixes = params.get("topic_prefixes", "").split(",").toList,
    consumerGroup = params.get("consumer_group", "wdqs_streaming_updater"),
    parallelism = params.getInt("consumer_parallelism", 1),
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
    latencyTrackingInterval = optionalIntArg("latency_tracking_interval")
  )
  val spuriousEventsDir: String = getStringParam("spurious_events_dir")
  val failedOpsDir: String = getStringParam("failed_ops_dir")
  val lateEventsDir: String = getStringParam("late_events_dir")

  val checkpointingMode: CheckpointingMode = if (params.getBoolean("exactly_once", true)) {
    CheckpointingMode.EXACTLY_ONCE
  } else {
    CheckpointingMode.AT_LEAST_ONCE
  }
  val outputStreamConfig: UpdaterPipelineOutputStreamConfig =
    UpdaterPipelineOutputStreamConfig(outputKafkaBrokers, outputTopic, outputPartition, checkpointingMode)

  implicit def finiteDuration2Int(fd: FiniteDuration): Int = fd.toMillis.intValue

  private def optionalIntArg(paramName: String)(implicit params: ParameterTool): Option[Int] = {
    if (params.has(paramName)) {
      Some(params.getInt(paramName))
    } else {
      None
    }
  }
}

object UpdaterConfig {
  def apply(args: Array[String]): UpdaterConfig = new UpdaterConfig(args)
}

sealed case class UpdaterPipelineGeneralConfig(hostname: String,
                                               reorderingWindowLengthMs: Int,
                                               reorderingOpParallelism: Option[Int],
                                               decideMutationOpParallelism: Option[Int],
                                               generateDiffParallelism: Int,
                                               generateDiffTimeout: Long,
                                               wikibaseRepoThreadPoolSize: Int,
                                               outputParallelism: Int = 1,
                                               outputOperatorNameAndUuid: String
                                              )

sealed case class UpdaterPipelineInputEventStreamConfig(kafkaBrokers: String,
                                                        consumerGroup: String,
                                                        revisionCreateTopicName: String,
                                                        pageDeleteTopicName: String,
                                                        pageUndeleteTopicName: String,
                                                        suppressedDeleteTopicName: String,
                                                        topicPrefixes: List[String],
                                                        parallelism: Int,
                                                        maxLateness: Int,
                                                        idleness: Int)

sealed case class UpdaterPipelineOutputStreamConfig(
                                                     kafkaBrokers: String,
                                                     topic: String,
                                                     partition: Int,
                                                     checkpointingMode: CheckpointingMode
                                                   )

sealed case class UpdaterExecutionEnvironmentConfig(checkpointDir: String,
                                                    checkpointInterval: Int,
                                                    checkpointTimeout: Int,
                                                    minPauseBetweenCheckpoints: Int,
                                                    autoWMInterval: Int,
                                                    checkpointingMode: CheckpointingMode,
                                                    unalignedCheckpoints: Boolean,
                                                    networkBufferTimeout: Int,
                                                    latencyTrackingInterval: Option[Int])
