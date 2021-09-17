package org.wikidata.query.rdf.updater

import java.util.Properties

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, SinkFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}

case class OutputStreams(
                          mutationSink: SinkFunction[MutationDataChunk],
                          lateEventsSink: SinkFunction[InputEvent] = new DiscardingSink[InputEvent],
                          spuriousEventsSink: SinkFunction[IgnoredMutation] = new DiscardingSink[IgnoredMutation],
                          failedOpsSink: SinkFunction[FailedOp] = new DiscardingSink[FailedOp]
                        )

class OutputStreamsBuilder(outputStreamsConfig: UpdaterPipelineOutputStreamConfig, httpClientConfig: HttpClientConfig) {
  def build: OutputStreams = {
    OutputStreams(
      mutationOutput,
      prepareSideOutputStream[InputEvent](JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
        outputStreamsConfig.schemaRepos, httpClientConfig),
      prepareSideOutputStream[IgnoredMutation](JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema,
        outputStreamsConfig.schemaRepos, httpClientConfig),
      prepareSideOutputStream[FailedOp](JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema,
        outputStreamsConfig.schemaRepos, httpClientConfig))
  }

  private def prepareSideOutputStream[E](stream: String, schema: String, schemaRepos: List[String], httpClientConfig: HttpClientConfig): SinkFunction[E] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.sideOutputsKafkaBrokers.getOrElse(outputStreamsConfig.kafkaBrokers))
    val topic = outputStreamsConfig.outputTopicPrefix.getOrElse("") + stream
    new FlinkKafkaProducer[E](
      topic,
      new SideOutputSerializationSchema[E](None, topic, stream, schema, outputStreamsConfig.sideOutputsDomain,
        outputStreamsConfig.eventStreamConfigEndpoint, schemaRepos, httpClientConfig),
      producerConfig,
      // force at least once semantic (WMF event platform does not seem to support kafka transactions yet)
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
  }

  def mutationOutput: SinkFunction[MutationDataChunk] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.kafkaBrokers)
    // Flink defaults is 1hour but wmf kafka uses the default value of 15min for transaction.max.timeout.ms
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("batch.size", "250000")
    producerConfig.setProperty("compression.type", "gzip")
    new FlinkKafkaProducer[MutationDataChunk](
      outputStreamsConfig.topic,
      new MutationEventDataSerializationSchema(outputStreamsConfig.topic, outputStreamsConfig.partition),
      producerConfig,
      outputStreamsConfig.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      })
  }
}
