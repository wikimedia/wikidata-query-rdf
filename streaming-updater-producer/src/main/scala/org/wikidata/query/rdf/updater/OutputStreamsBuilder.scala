package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}

import java.util.Properties

case class OutputStreams(
                          mutationSink: SinkWrapper[MutationDataChunk],
                          lateEventsSink: SinkWrapper[InputEvent],
                          spuriousEventsSink: SinkWrapper[InconsistentMutation],
                          failedOpsSink: SinkWrapper[FailedOp]
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
class OutputStreamsBuilder(outputStreamsConfig: UpdaterPipelineOutputStreamConfig, httpClientConfig: HttpClientConfig) {
  def build: OutputStreams = {
    OutputStreams(
      mutationOutput(outputStreamsConfig),
      prepareSideOutputStream[InputEvent](JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
        outputStreamsConfig.schemaRepos, httpClientConfig, "late-events-output"),
      prepareSideOutputStream[InconsistentMutation](JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema,
        outputStreamsConfig.schemaRepos, httpClientConfig, "spurious-events-output"),
      prepareSideOutputStream[FailedOp](JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema,
        outputStreamsConfig.schemaRepos, httpClientConfig, "failed-events-output"))
  }

  private def prepareSideOutputStream[E](stream: String, schema: String, schemaRepos: List[String],
                                         httpClientConfig: HttpClientConfig, operatorNameAndUuid: String): SinkWrapper[E] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.sideOutputsKafkaBrokers.getOrElse(outputStreamsConfig.kafkaBrokers))
    val topic = outputStreamsConfig.outputTopicPrefix.getOrElse("") + stream
    val sideOutputSerializationSchema = new SideOutputSerializationSchema[E](None, topic, stream, schema, outputStreamsConfig.sideOutputsDomain,
      outputStreamsConfig.eventStreamConfigEndpoint, schemaRepos, httpClientConfig)

    if (outputStreamsConfig.useNewFlinkKafkaApi) {
      sideOutputWithKafkaSink(producerConfig, sideOutputSerializationSchema, operatorNameAndUuid)
    } else {
      sideOutputWithFlinkKafkaConsumer(producerConfig, topic, sideOutputSerializationSchema, operatorNameAndUuid)
    }
  }

  private def sideOutputWithFlinkKafkaConsumer[E](producerConfig: Properties, defaultTopic: String,
                                                  sideOutputSerializationSchema: SideOutputSerializationSchema[E],
                                                  operatorNameAndUuid: String): SinkWrapper[E] = {
    SinkWrapper(Left(new FlinkKafkaProducer[E](
      defaultTopic,
      sideOutputSerializationSchema.asDeprecatedKafkaSerializationSchema(),
      producerConfig,
      // force at least once semantic (WMF event platform does not seem to support kafka transactions yet)
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)), operatorNameAndUuid)
  }

  private def sideOutputWithKafkaSink[E](producerConfig: Properties,
                                         serializer: KafkaRecordSerializationSchema[E], operatorNameAndUuid: String): SinkWrapper[E] = {
    SinkWrapper(Right(KafkaSink.builder[E]()
      .setKafkaProducerConfig(producerConfig)
      .setRecordSerializer(serializer)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()), operatorNameAndUuid)

  }

  def mutationOutput(outputStreamConfig: UpdaterPipelineOutputStreamConfig): SinkWrapper[MutationDataChunk] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.kafkaBrokers)
    // Flink defaults is 1hour but wmf kafka uses the default value of 15min for transaction.max.timeout.ms
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("batch.size", "250000")
    producerConfig.setProperty("compression.type", "gzip")

    if (outputStreamConfig.useNewFlinkKafkaApi) {
      withKafkaSink(outputStreamConfig.transactionalIdPrefix, producerConfig)
    } else {
      withDeprecadedFlinkKafkaProducer(outputStreamConfig.transactionalIdPrefix, producerConfig)
    }
  }

  def withDeprecadedFlinkKafkaProducer(transactionalIdPrefix: String, producerConfig: Properties): SinkWrapper[MutationDataChunk] = {
    val producer = new FlinkKafkaProducer[MutationDataChunk](
      outputStreamsConfig.topic,
      new MutationEventDataSerializationSchema(outputStreamsConfig.topic, outputStreamsConfig.partition),
      producerConfig,
      outputStreamsConfig.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      })

    producer.setTransactionalIdPrefix(transactionalIdPrefix)

    if (outputStreamsConfig.ignoreFailuresAfterTransactionTimeout) {
      // workaround for https://issues.apache.org/jira/browse/FLINK-16419
      producer.ignoreFailuresAfterTransactionTimeout();
    }
    producer.setTransactionalIdPrefix(transactionalIdPrefix)
    // Re-use transactionalIdPrefix of the operator name and UUID
    SinkWrapper(Left(producer), transactionalIdPrefix)
  }


  def withKafkaSink(transactionalPrefixId: String, producerConfig: Properties): SinkWrapper[MutationDataChunk] = {
    val serializer = new MutationEventDataSerializationSchema(outputStreamsConfig.topic, outputStreamsConfig.partition)
    val kafkaSink = KafkaSink.builder[MutationDataChunk]()
      .setDeliveryGuarantee(outputStreamsConfig.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => DeliveryGuarantee.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => DeliveryGuarantee.AT_LEAST_ONCE
      })
      .setKafkaProducerConfig(producerConfig)
      .setTransactionalIdPrefix(transactionalPrefixId)
      .setRecordSerializer(serializer.asKafkaRecordSerializationSchema())
      .build()
    SinkWrapper(Right(kafkaSink), "mutation-output")
  }
}
