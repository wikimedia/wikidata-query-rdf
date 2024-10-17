package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.wikidata.query.rdf.updater.config.UpdaterPipelineOutputStreamConfig

import java.util.Properties

case class DirectSinkWrapper[E](sink: Either[SinkFunction[E], Sink[E]], nameAndUuid: String, outputParallelism: Int) extends SinkWrapper[E] {
  def attachStream(stream: DataStream[E]): Unit = {
    val op = sink match {
      case Left(s) => stream.addSink(s)
      case Right(s) => stream.sinkTo(s)
    }
    op.uid(nameAndUuid)
      .name(nameAndUuid)
      .setParallelism(outputParallelism)
  }
}

class OutputStreamsBuilder(outputStreamsConfig: UpdaterPipelineOutputStreamConfig,
                           eventPlatformFactory: EventPlatformFactory,
                           outputParallelism: Int) {
  def build: OutputStreams = {
    val streamToTopic: Map[String, String] = outputStreamsConfig.subgraphKafkaTopics ++ Map(outputStreamsConfig.mainStream -> outputStreamsConfig.topic)
    val mutationSink = mutationOutput(outputStreamsConfig, streamToTopic)
    if (outputStreamsConfig.produceSideOutputs) {
      OutputStreams(
        mutationSink = mutationSink,
        lateEventsSink =
          Some(prepareSideOutputStream[InputEvent](JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
            eventPlatformFactory, "late-events-output", outputStreamsConfig)),
        spuriousEventsSink =
          Some(prepareSideOutputStream[InconsistentMutation](JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema,
            eventPlatformFactory, "spurious-events-output", outputStreamsConfig)),
        failedOpsSink =
          Some(prepareSideOutputStream[FailedOp](JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema,
            eventPlatformFactory, "failed-events-output", outputStreamsConfig)))
    } else {
      OutputStreams(mutationSink)
    }
  }

  private def prepareSideOutputStream[E](stream: String, schema: String,
                                         eventPlatformFactory: EventPlatformFactory, operatorNameAndUuid: String,
                                         outputStreamsConfig: UpdaterPipelineOutputStreamConfig): SinkWrapper[E] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.sideOutputsKafkaBrokers.getOrElse(outputStreamsConfig.kafkaBrokers))
    outputStreamsConfig.producerProperties.foreach { case (k, v) => producerConfig.setProperty(k, v) }
    val topic = outputStreamsConfig.outputTopicPrefix.getOrElse("") + stream
    val sideOutputSerializationSchema = new SideOutputSerializationSchema[E](topic, stream, schema, outputStreamsConfig.sideOutputsDomain,
      outputStreamsConfig.emitterId, eventPlatformFactory)

    sideOutputWithKafkaSink(producerConfig, sideOutputSerializationSchema, operatorNameAndUuid)
  }

  private def sideOutputWithKafkaSink[E](producerConfig: Properties,
                                         serializer: KafkaRecordSerializationSchema[E], operatorNameAndUuid: String): SinkWrapper[E] = {
    DirectSinkWrapper(Right(KafkaSink.builder[E]()
      .setKafkaProducerConfig(producerConfig)
      .setRecordSerializer(serializer)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()), f"KafkaSink-$operatorNameAndUuid", outputParallelism)

  }

  def mutationOutput(outputStreamConfig: UpdaterPipelineOutputStreamConfig, streamToTopic: Map[String, String]): SinkWrapper[MutationDataChunk] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.kafkaBrokers)
    // Flink defaults is 1hour but wmf kafka uses the default value of 15min for transaction.max.timeout.ms
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    outputStreamConfig.producerProperties.foreach { case (k, v) => producerConfig.setProperty(k, v) }
    withKafkaSink(s"${outputStreamConfig.topic}:${outputStreamConfig.partition}", producerConfig, streamToTopic)
  }

  def withKafkaSink(
                     transactionalPrefixId: String,
                     producerConfig: Properties,
                     streamToTopic: Map[String, String]
                   ): SinkWrapper[MutationDataChunk] = {
    val serializer = new MutationEventDataSerializationSchema(streamToTopic, outputStreamsConfig.partition)
    val kafkaSink = KafkaSink.builder[MutationDataChunk]()
      .setDeliveryGuarantee(outputStreamsConfig.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => DeliveryGuarantee.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => DeliveryGuarantee.AT_LEAST_ONCE
      })
      .setKafkaProducerConfig(producerConfig)
      .setTransactionalIdPrefix(transactionalPrefixId)
      .setRecordSerializer(serializer)
      .build()
    DirectSinkWrapper(Right(kafkaSink), f"mutation-output", outputParallelism)
  }
}

