package org.wikidata.query.rdf.updater

import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.updater.config.HttpClientConfig
import org.wikimedia.eventutilities.core.SerializableClock
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventStreamConfig, EventStreamFactory, JsonEventGenerator}
import org.wikimedia.eventutilities.core.http.BasicHttpClient
import org.wikimedia.eventutilities.core.json.{JsonLoader, JsonSchemaLoader}
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.flink.stream.EventDataStreamFactory

import java.net.{URI, URL}
import java.time.{Clock, Instant}
import java.util
import java.util.Collections
import scala.collection.JavaConverters._

class EventPlatformFactory(streamConfigUri: String,
                           schemaBaseUris: List[String],
                           httpClientConfig: HttpClientConfig,
                           val clock: Clock = Clock.systemUTC()) extends Serializable {

  @transient
  lazy val httpClient: BasicHttpClient = {
    val timeout: Int = httpClientConfig.httpTimeout.getOrElse(HttpClientUtils.TIMEOUT.toMillis.intValue())
    val builder = BasicHttpClient.builder()
    HttpClientUtils.configureHttpClient(builder.httpClientBuilder(),
      HttpClientUtils.createPooledConnectionManager(timeout),
      None.orNull, httpClientConfig.httpRoutes.orNull, timeout,
      httpClientConfig.userAgent)
    builder.build()
  }

  @transient
  lazy val resLoader: ResourceLoader = ResourceLoader.builder()
    .withHttpClient(httpClient)
    .setBaseUrls(schemaBaseUris.map(new URL(_)).asJava)
    .build()

  @transient
  lazy val jsonLoader: JsonLoader = new JsonLoader(resLoader)

  @transient
  lazy val eventStreamConfig: EventStreamConfig = EventStreamConfig.builder()
    .setEventStreamConfigLoader(streamConfigUri)
    .setEventServiceToUriMap(Collections.emptyMap(): util.Map[String, URI])
    .setJsonLoader(jsonLoader)
    .build()

  @transient
  lazy val jsonSchemaLoader: JsonSchemaLoader = JsonSchemaLoader.build(resLoader)

  @transient
  lazy val eventSchemaLoader: EventSchemaLoader = EventSchemaLoader.builder()
    .setJsonSchemaLoader(jsonSchemaLoader)
    .build()

  @transient
  lazy val eventStreamFactory: EventStreamFactory = EventStreamFactory.builder()
    .setEventStreamConfig(eventStreamConfig)
    .setEventSchemaLoader(eventSchemaLoader)
    .build()

  @transient
  lazy val jsonEventGenerator: JsonEventGenerator = JsonEventGenerator.builder()
    .schemaLoader(eventSchemaLoader)
    .ingestionTimeClock(new SerializableClock {
      override def get(): Instant = clock.instant()
    })
    .eventStreamConfig(eventStreamConfig)
    .build()

  @transient
  lazy val eventDataStreamFactory: EventDataStreamFactory = EventDataStreamFactory.builder()
      .eventStreamFactory(eventStreamFactory)
      .generator(jsonEventGenerator)
      .build()
}
