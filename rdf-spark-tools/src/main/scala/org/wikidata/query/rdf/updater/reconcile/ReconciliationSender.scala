package org.wikidata.query.rdf.updater.reconcile

import java.io.IOException
import java.net.URI
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ByteArrayEntity, ContentType}
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.util.EntityUtils
import org.wikidata.query.rdf.tool.MapperUtils
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent

import java.time.Instant

class ReconciliationSender(httpClient: HttpClient,
                           eventServiceUri: URI,
                           batchSize: Int = 100,
                           jsonObjectMapper: ObjectMapper = MapperUtils.getObjectMapper,
                           retries: Int = 3,
                           retryWaitMs: Long = 500,
                           clock: () => Instant = () => Instant.now()) {

  def send(events: Iterable[ReconcileEvent]): Unit = {
    events.grouped(batchSize).foreach(batch => withRetry()(() => sendBatch(batch, httpClient)))
  }

  @tailrec
  private def withRetry(nretry: Int = retries)(func: () => Unit): Unit = {
    Try {
      func()
    } match {
      case Success(_: Any) => // success
      case Failure(_: IOException) if nretry > 0 => Thread.sleep(retryWaitMs); withRetry(nretry - 1)(func)
      case Failure(e) => throw new IOException("Failed to send events: " + e.getMessage, e)
    }
  }

  private def sendBatch(events: Iterable[ReconcileEvent], httpClient: HttpClient): Unit = {
    val post = new HttpPost(eventServiceUri)
    // force a "fresh" event-time, we don't want these events to be considered late or they might be collected again...
    post.setEntity(new ByteArrayEntity(jsonObjectMapper.writeValueAsBytes(events.map(e => e.overrideEventTime(clock())).toArray), ContentType.APPLICATION_JSON))
    val response = httpClient.execute(post)
    EntityUtils.consume(response.getEntity)
    if (!validResponse(response.getStatusLine)) {
      throw new IOException("Received unexpected error " + response.getStatusLine + " from " + eventServiceUri)
    }
  }

  def validResponse(statusLine: StatusLine): Boolean = Seq(201, 202).contains(statusLine.getStatusCode)
}
