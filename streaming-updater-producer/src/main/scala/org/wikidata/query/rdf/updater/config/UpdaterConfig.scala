package org.wikidata.query.rdf.updater.config

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.{FederatedUrisScheme, UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

class UpdaterConfig(args: Array[String]) extends BaseConfig()(BaseConfig.params(args)) {
  private val log = LoggerFactory.getLogger(this.getClass)
  val checkpointDir: String = getStringParam("checkpoint_dir")
  private val hostName: String = getStringParam("hostname")
  val jobName: String = getStringParam("job_name")
  val inputKafkaBrokers: String = getStringParam("brokers")
  private val outputKafkaBrokers: String = params.get("output_brokers", inputKafkaBrokers)
  val outputTopic: String = getStringParam("output_topic")
  val outputPartition: Int = params.getInt("output_topic_partition")
  val entityNamespaces: Set[Long] = params.get("entity_namespaces", "").split(",").map(_.trim).filterNot(_.isEmpty).map(_.toLong).toSet
  val mediaInfoEntityNamespaces: Set[Long] = params.get("mediainfo_entity_namespaces", "").split(",").map(_.trim).filterNot(_.isEmpty).map(_.toLong).toSet
  val entityDataPath: String = params.get("wikibase_entitydata_path", Uris.DEFAULT_ENTITY_DATA_PATH)
  val printExecutionPlan: Boolean = params.getBoolean("print_execution_plan", false)
  if (entityNamespaces.isEmpty && mediaInfoEntityNamespaces.isEmpty) {
    throw new IllegalArgumentException("entity_namespaces and/or mediainfo_entity_namespaces")
  }
  private val jobParallelism = params.getInt("parallelism", 1)

  // Assuming we want to process 50evt/sec (~2x the realtime speed) and the avg response time is 100ms and that we make 2 requests per events (diffs)
  // this is 50*2 = 100rps, 100*0.1 = 10 concurrent requests
  private val mwMaxConcurrentRequests: Int = params.getInt("mediawiki_max_concurrent_requests", 10)

  val transactionalIdPrefix = s"$outputTopic:$outputPartition"
  val generalConfig: UpdaterPipelineGeneralConfig = UpdaterPipelineGeneralConfig(
    hostname = hostName,
    jobName = jobName,
    entityNamespaces = entityNamespaces ++ mediaInfoEntityNamespaces,
    entityDataPath = entityDataPath,
    reorderingWindowLengthMs = params.getInt("reordering_window_length", 1 minute),

    generateDiffTimeout = params.getLong("generate_diff_timeout", 5.minutes.toMillis),
    wikibaseRepoThreadPoolSize = wikibaseRepoThreadPoolSize(jobParallelism, mwMaxConcurrentRequests),
    httpClientConfig = HttpClientConfig(
      httpRoutes = optionalStringArg("http_routes"),
      httpTimeout = optionalIntArg("http_timeout"),
      userAgent = params.get("user_agent", HttpClientUtils.WDQS_DEFAULT_UA)
    ),
    urisScheme = getStringParam("uris_scheme") match {
      case "commons" =>
        new FederatedUrisScheme(
          UrisSchemeFactory.forCommons(optionalUriArg("commons_concept_uri").getOrElse(UrisSchemeFactory.commonsUri(hostName))),
          UrisSchemeFactory.forWikidata(optionalUriArg("wikidata_concept_uri").getOrElse(UrisSchemeFactory.wikidataUri(UrisSchemeFactory.WIKIDATA_HOSTNAME))))
      case "wikidata" =>
        UrisSchemeFactory.forWikidata(optionalUriArg("wikidata_concept_uri").getOrElse(UrisSchemeFactory.wikidataUri(hostName)))
      case scheme: Any => throw new IllegalArgumentException(s"Unknown uris_scheme: $scheme")
    },
    acceptableMediawikiLag = params.getInt("acceptable_mediawiki_lag", 10) seconds
  )

  private val useNewFlinkKafkaApi: Boolean = params.getBoolean("use_new_flink_kafka_api", false)
  val inputEventStreamConfig: UpdaterPipelineInputEventStreamConfig = UpdaterPipelineInputEventStreamConfig(kafkaBrokers = inputKafkaBrokers,
    inputKafkaTopics = getInputKafkaTopics,
    consumerGroup = getStringParam("consumer_group"),
    maxLateness = params.getInt("max_lateness", 1 minute),
    idleness = params.getInt("input_idleness", 1 minute),
    mediaInfoEntityNamespaces = mediaInfoEntityNamespaces,
    mediaInfoRevisionSlot = params.get("mediainfo_revision_slot", "mediainfo"),
    useNewFlinkKafkaApi = useNewFlinkKafkaApi
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
    parallelism = jobParallelism
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
      schemaRepos = params.get(
        "schema_repositories",
        "https://schema.wikimedia.org/repositories/primary/jsonschema,https://schema.wikimedia.org/repositories/secondary/jsonschema"
      ).split(",")
        .map(_.trim)
        .toList,
      ignoreFailuresAfterTransactionTimeout = params.getBoolean("ignore_failures_after_transaction_timeout", false),
      transactionalIdPrefix = transactionalIdPrefix,
      useNewFlinkKafkaApi = useNewFlinkKafkaApi,
      produceSideOutputs = params.getBoolean("produce_side_outputs", true)
    )

  implicit def finiteDuration2Int(fd: FiniteDuration): Int = fd.toMillis.intValue

  def wikibaseRepoThreadPoolSize(fetchOpParallelism: Int, maxTotalConcurrentRequests: Int): Int = {
    optionalIntArg("wikibase_repo_thread_pool_size") match {
      case Some(size) => size
      case None =>
        if (maxTotalConcurrentRequests < fetchOpParallelism) {
          throw new IllegalArgumentException (s"The expected concurrency limits of $maxTotalConcurrentRequests cannot be achieved " +
            s"with a parallelism of $fetchOpParallelism, please set --parallelism to at most $maxTotalConcurrentRequests")
        }
        val threadPoolSize = maxTotalConcurrentRequests / fetchOpParallelism
        val actualLimit = threadPoolSize * fetchOpParallelism;
        if (actualLimit != maxTotalConcurrentRequests) {
          log.warn(s"The concurrency limit of $maxTotalConcurrentRequests is not a multiple of $fetchOpParallelism, the actual limit used will be $actualLimit")
        }
        threadPoolSize
    }
  }
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
                                               httpClientConfig: HttpClientConfig,
                                               urisScheme: UrisScheme,
                                               acceptableMediawikiLag: FiniteDuration
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
                                                        mediaInfoEntityNamespaces: Set[Long],
                                                        mediaInfoRevisionSlot: String,
                                                        useNewFlinkKafkaApi: Boolean
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
                                                     schemaRepos: List[String],
                                                     ignoreFailuresAfterTransactionTimeout: Boolean,
                                                     transactionalIdPrefix: String,
                                                     useNewFlinkKafkaApi: Boolean,
                                                     produceSideOutputs: Boolean
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
