package org.wikidata.query.rdf.updater

import java.lang
import java.net.{URI, URL}
import java.time.{Clock, Instant}
import java.util.function.{Consumer, Supplier}
import java.util.Collections

import scala.collection.JavaConverters.seqAsJavaListConverter

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.kafka.clients.producer.ProducerRecord
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.updater.config.HttpClientConfig
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventStreamConfig, JsonEventGenerator}
import org.wikimedia.eventutilities.core.http.BasicHttpClient
import org.wikimedia.eventutilities.core.json.{JsonLoader, JsonSchemaLoader}
import org.wikimedia.eventutilities.core.util.ResourceLoader

class SideOutputSerializationSchema[E](recordTimeClock: Option[() => Instant],
                                       topic: String,
                                       stream: String,
                                       schema: String,
                                       sideOutputsDomain: String,
                                       eventStreamConfigEndpoint: String,
                                       schemaRepos: List[String],
                                       httpClientConfig: HttpClientConfig) extends KafkaSerializationSchema[E] {

  private def getRecordClock(): () => Instant = {
    recordTimeClock match {
      case Some(recTimeClock) => recTimeClock
      case None => {
        val systemClock: Clock = Clock.systemUTC()
        () => systemClock.instant()
      }
    }
  }

  lazy val clock: () => Instant = getRecordClock()

  private def getEventGenerator(): JsonEventGenerator = {
    val timeout: Int = httpClientConfig.httpTimeout.getOrElse(HttpClientUtils.TIMEOUT.toMillis.intValue())
    val client: CloseableHttpClient = HttpClientUtils.createHttpClient(
      HttpClientUtils.createPooledConnectionManager(timeout),
      None.orNull,
      httpClientConfig.httpRoutes.orNull,
      timeout,
      httpClientConfig.userAgent)

    val resLoader: ResourceLoader = ResourceLoader.builder()
      .withHttpClient(new BasicHttpClient(client))
      .setBaseUrls(schemaRepos.map(new URL(_)).asJava)
      .build()
    val jsonLoader = new JsonLoader(resLoader)
    JsonEventGenerator.builder()
      .schemaLoader(EventSchemaLoader.builder()
        .setJsonSchemaLoader(new JsonSchemaLoader(jsonLoader))
        .build())
      .eventStreamConfig(EventStreamConfig.builder()
        .setEventStreamConfigLoader(eventStreamConfigEndpoint)
        .setEventServiceToUriMap(Collections.emptyMap(): java.util.Map[String, URI])
        .setJsonLoader(new JsonLoader(resLoader))
        .build())
     .ingestionTimeClock(new Supplier[Instant] {
        override def get(): Instant = clock()
      }).build()
  }

  lazy val jsonEventGenerator: JsonEventGenerator = getEventGenerator()
  override def serialize(element: E, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    lazy val jsonEncoders = new JsonEncoders(sideOutputsDomain)
    val recordTime: Instant = clock()
    val eventCreator: Consumer[ObjectNode] = element match {
      case e: InputEvent => jsonEncoders.lapsedActionEvent(e)
      case e: FailedOp => jsonEncoders.fetchFailureEvent(e)
      case e: IgnoredMutation => jsonEncoders.stateInconsistencyEvent(e)
      case _ => throw new IllegalArgumentException("Unknown input type [" + element.getClass + "]")
    }
    val jsonEvent: ObjectNode = jsonEventGenerator.generateEvent(stream, schema, eventCreator, recordTime)
    val eventData: Array[Byte] = jsonEventGenerator.serializeAsBytes(jsonEvent)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, null, recordTime.toEpochMilli, null, eventData) // scalastyle:ignore null
  }
}
