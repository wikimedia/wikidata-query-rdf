package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}

import java.util.Properties

case class OutputStreams(
                          mutationSink: SinkWrapper[MutationDataChunk],
                          subgraphMutationSinks: Map[String, SinkWrapper[MutationDataChunk]] = Map.empty,
                          lateEventsSink: Option[SinkWrapper[InputEvent]] = None,
                          spuriousEventsSink: Option[SinkWrapper[InconsistentMutation]] = None,
                          failedOpsSink: Option[SinkWrapper[FailedOp]] = None
                        )

case class SinkWrapper[E](sink: Either[SinkFunction[E], Sink[E]], nameAndUuid: String) {
  def attachStream(stream: DataStream[E]): DataStreamSink[E] = {
    val op = sink match {
      case Left(s) => stream.addSink(s)
      case Right(s) => stream.sinkTo(s)
    }
    op.uid(nameAndUuid)
      .name(nameAndUuid)
  }
}

class OutputStreamsBuilder(outputStreamsConfig: UpdaterPipelineOutputStreamConfig, httpClientConfig: HttpClientConfig, subgraphStreams: Iterable[String]) {
  def build: OutputStreams = {
    val mutationSink = mutationOutput(outputStreamsConfig, outputStreamsConfig.topic)
    val subgraphMutationSinks = subgraphStreams
      .map(subgraphStream => subgraphStream -> mutationOutput(
        outputStreamsConfig,
        outputStreamsConfig.subgraphKafkaTopics(subgraphStream),
        nameAndUuidSuffix = f"-$subgraphStream")
      ).toMap
    if (outputStreamsConfig.produceSideOutputs) {
      OutputStreams(
        mutationSink = mutationSink,
        subgraphMutationSinks = subgraphMutationSinks,
        lateEventsSink =
          Some(prepareSideOutputStream[InputEvent](JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
            outputStreamsConfig.schemaRepos, httpClientConfig, "late-events-output")),
        spuriousEventsSink =
          Some(prepareSideOutputStream[InconsistentMutation](JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema,
            outputStreamsConfig.schemaRepos, httpClientConfig, "spurious-events-output")),
        failedOpsSink =
          Some(prepareSideOutputStream[FailedOp](JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema,
            outputStreamsConfig.schemaRepos, httpClientConfig, "failed-events-output")))
    } else {
      OutputStreams(mutationSink, subgraphMutationSinks = subgraphMutationSinks)
    }
  }

  private def prepareSideOutputStream[E](stream: String, schema: String, schemaRepos: List[String],
                                         httpClientConfig: HttpClientConfig, operatorNameAndUuid: String): SinkWrapper[E] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.sideOutputsKafkaBrokers.getOrElse(outputStreamsConfig.kafkaBrokers))
    val topic = outputStreamsConfig.outputTopicPrefix.getOrElse("") + stream
    val sideOutputSerializationSchema = new SideOutputSerializationSchema[E](None, topic, stream, schema, outputStreamsConfig.sideOutputsDomain,
      outputStreamsConfig.emitterId, outputStreamsConfig.eventStreamConfigEndpoint, schemaRepos, httpClientConfig)

    sideOutputWithKafkaSink(producerConfig, sideOutputSerializationSchema, operatorNameAndUuid)
  }

  private def sideOutputWithKafkaSink[E](producerConfig: Properties,
                                         serializer: KafkaRecordSerializationSchema[E], operatorNameAndUuid: String): SinkWrapper[E] = {
    SinkWrapper(Right(KafkaSink.builder[E]()
      .setKafkaProducerConfig(producerConfig)
      .setRecordSerializer(serializer)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()), f"KafkaSink-$operatorNameAndUuid")

  }

  def mutationOutput(outputStreamConfig: UpdaterPipelineOutputStreamConfig, topic: String, nameAndUuidSuffix: String = ""): SinkWrapper[MutationDataChunk] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.kafkaBrokers)
    // Flink defaults is 1hour but wmf kafka uses the default value of 15min for transaction.max.timeout.ms
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    outputStreamConfig.producerProperties.foreach { case (k, v) => producerConfig.setProperty(k, v) }
    withKafkaSink(s"$topic:${outputStreamConfig.partition}", producerConfig, topic, nameAndUuidSuffix = nameAndUuidSuffix)
  }

  def withKafkaSink(
                     transactionalPrefixId: String,
                     producerConfig: Properties,
                     topic: String,
                     nameAndUuidSuffix: String = ""
                   ): SinkWrapper[MutationDataChunk] = {
    val serializer = new MutationEventDataSerializationSchema(topic, outputStreamsConfig.partition)
    val kafkaSink = KafkaSink.builder[MutationDataChunk]()
      .setDeliveryGuarantee(outputStreamsConfig.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => DeliveryGuarantee.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => DeliveryGuarantee.AT_LEAST_ONCE
      })
      .setKafkaProducerConfig(producerConfig)
      .setTransactionalIdPrefix(transactionalPrefixId)
      .setRecordSerializer(serializer.asKafkaRecordSerializationSchema())
      .build()
    SinkWrapper(Right(kafkaSink), f"mutation-output$nameAndUuidSuffix")
  }
}
