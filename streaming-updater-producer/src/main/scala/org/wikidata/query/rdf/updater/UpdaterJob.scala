package org.wikidata.query.rdf.updater

import java.net.URI
import java.time.Clock

import scala.collection.JavaConverters.setAsJavaSetConverter

import org.apache.flink.api.common.restartstrategy.RestartStrategies.FailureRateRestartStrategyConfiguration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{BaseConfig, UpdaterConfig, UpdaterExecutionEnvironmentConfig}

object UpdaterJob {
  val DEFAULT_CLOCK: Clock = Clock.systemUTC()

  def main(args: Array[String]): Unit = {
    val config = UpdaterConfig(args)
    val generalConfig = config.generalConfig

    val outputStreamsBuilder: OutputStreamsBuilder = new OutputStreamsBuilder(config.outputStreamConfig, generalConfig.httpClientConfig)
    val uris: Uris = new WikibaseRepository.Uris(new URI(s"https://${generalConfig.hostname}"),
      generalConfig.entityNamespaces.map(long2Long).asJava,
      WikibaseRepository.Uris.DEFAULT_ENTITY_DATA_PATH, // unused by the pipeline
      generalConfig.entityDataPath)
    implicit val env: StreamExecutionEnvironment = prepareEnv(config.environmentConfig)

    val incomingStreams = IncomingStreams.buildIncomingStreams(config.inputEventStreamConfig, uris, DEFAULT_CLOCK)
    UpdaterPipeline.configure(
        opts = generalConfig,
        incomingStreams = incomingStreams,
        outputStreams = outputStreamsBuilder.build,
        wikibaseRepositoryGenerator = rc => WikibaseEntityRevRepository(uris, generalConfig.httpClientConfig, rc.getMetricGroup))
    env.execute(generalConfig.jobName)
  }

  private def prepareEnv(environmentOption: UpdaterExecutionEnvironmentConfig): StreamExecutionEnvironment = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(BaseConfig.MAX_PARALLELISM)
    env.setStateBackend(UpdaterStateConfiguration.newStateBackend(environmentOption.checkpointDir))
    env.enableCheckpointing(environmentOption.checkpointInterval, environmentOption.checkpointingMode)
    env.getCheckpointConfig.setCheckpointTimeout(environmentOption.checkpointTimeout)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(environmentOption.minPauseBetweenCheckpoints)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)
    env.getCheckpointConfig.enableUnalignedCheckpoints(environmentOption.unalignedCheckpoints)
    env.setRestartStrategy(new FailureRateRestartStrategyConfiguration(
      environmentOption.restartFailureRateMaxPerInternal,
      environmentOption.restartFailureRateInterval,
      environmentOption.restartFailureRateDelay
    ))
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getConfig.setAutoWatermarkInterval(environmentOption.autoWMInterval)
    env.getConfig.enableObjectReuse()
    env.setBufferTimeout(environmentOption.networkBufferTimeout)
    environmentOption.latencyTrackingInterval.foreach(l => env.getConfig.setLatencyTrackingInterval(l))
    env.setParallelism(environmentOption.parallelism)
    env
  }

}

