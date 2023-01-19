package org.wikidata.query.rdf.updater.reconcile

import java.io.IOException
import java.net.URI
import java.time.Instant
import java.util.{function, UUID}

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.commons.io.IOUtils
import org.apache.http.HttpVersion
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpUriRequest}
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.message.BasicStatusLine
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.tool.MapperUtils
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta, ReconcileEvent}
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent.Action
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventSchemaValidator}
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader
import org.wikimedia.eventutilities.core.util.ResourceLoader

@deprecated("Workaround scalamock & deprecated http-client components", "1.0")
class ReconciliationSenderUnitTest extends AnyFlatSpec with Matchers with MockFactory {
  private val now: Instant = Instant.now();
  private val evtId: String = UUID.randomUUID().toString
  private val requestId: String = UUID.randomUUID().toString
  private val domain: String = "mydomain"
  private val mystream: String = "mystream"
  private val defaultLoader: function.Function[URI, Array[Byte]] = new function.Function[URI, Array[Byte]]() {
    override def apply(u: URI): Array[Byte] = IOUtils.toByteArray(u)
  }
  private val resLoader: ResourceLoader = ResourceLoader.builder()
    .setDefaultLoader(defaultLoader)
    .build()
  private val validator = new EventSchemaValidator(EventSchemaLoader.builder()
    .setJsonSchemaLoader(JsonSchemaLoader.build(resLoader))
    .build())

  "ReconciliationSender" should "send events after few retries" in {
    val httpClient = mock[HttpClient]
    val validResponse = stub[CloseableHttpResponse]
    val invalidResponse = stub[CloseableHttpResponse]
    val captureAllFailed = CaptureAll[HttpUriRequest]()
    val captureAllSuccess = CaptureAll[HttpUriRequest]()

    (invalidResponse.getStatusLine _).when().returns(new BasicStatusLine(HttpVersion.HTTP_1_1, 503, "not ready!"));
    (validResponse.getStatusLine _).when().returns(new BasicStatusLine(HttpVersion.HTTP_1_1, 202, "Good!"));

    (httpClient.execute(_: HttpUriRequest)) expects capture(captureAllFailed) returns invalidResponse repeat 2
    (httpClient.execute(_: HttpUriRequest)) expects capture(captureAllSuccess) returns validResponse noMoreThanOnce()
    val sender = new ReconciliationSender(httpClient, URI.create("https://my_endpoint/"), 2,
      MapperUtils.getObjectMapper, retries = 3, retryWaitMs = 1)
    sender.send(Seq(genEvent(), genEvent()))
    captureAllFailed.values should have length 2
    captureAllSuccess.values should have length 1
  }

  "ReconciliationSender" should "send batch of events compatible with their schema" in {
    val httpClient = mock[HttpClient]
    val validResponse = stub[CloseableHttpResponse]
    val captureAllSuccess = CaptureAll[HttpUriRequest]()

    (validResponse.getStatusLine _).when().returns(new BasicStatusLine(HttpVersion.HTTP_1_1, 202, "Good!"));

    (httpClient.execute(_: HttpUriRequest)) expects capture(captureAllSuccess) returns validResponse repeat 2
    val sender = new ReconciliationSender(httpClient, URI.create("https://my_endpoint/"), batchSize = 2,
      MapperUtils.getObjectMapper, retries = 3, retryWaitMs = 1)
    sender.send(Seq(genEvent(), genEvent(), genEvent()))
    val payload1 = captureAllSuccess.values.head.asInstanceOf[HttpPost].getEntity.asInstanceOf[ByteArrayEntity].getContent
    val payload2 = captureAllSuccess.values(1).asInstanceOf[HttpPost].getEntity.asInstanceOf[ByteArrayEntity].getContent
    val array = MapperUtils.getObjectMapper.reader().readTree(payload1)
    array shouldBe a[ArrayNode]
    (array.asInstanceOf[ArrayNode].elements().asScala map validator.validate filterNot (_.isSuccess) toList) shouldBe empty
    MapperUtils.getObjectMapper.readValue(payload2, classOf[Array[ReconcileEvent]]) should contain theSameElementsAs Seq(genEvent())
  }

  "ReconciliationSender" should "send abandon after few retries" in {
    val httpClient = mock[HttpClient]
    val validResponse = stub[CloseableHttpResponse]
    val invalidResponse = stub[CloseableHttpResponse]
    val captureAllFailed = CaptureAll[HttpUriRequest]()

    (invalidResponse.getStatusLine _).when().returns(new BasicStatusLine(HttpVersion.HTTP_1_1, 503, "not ready!"));
    (validResponse.getStatusLine _).when().returns(new BasicStatusLine(HttpVersion.HTTP_1_1, 202, "Good!"));

    (httpClient.execute(_: HttpUriRequest)) expects capture(captureAllFailed) returns invalidResponse anyNumberOfTimes()
    val sender = new ReconciliationSender(httpClient, URI.create("https://my_endpoint/"), batchSize = 2,
      MapperUtils.getObjectMapper, retries = 3, retryWaitMs = 1)
    Try {
      sender.send(Seq(genEvent(), genEvent()))
    } match {
      case Success(_) => fail("Should fail")
      case Failure(e: IOException) =>
        e.getMessage.shouldEqual("Failed to send events: Received unexpected error HTTP/1.1 503 not ready! from https://my_endpoint/")
      case Failure(e: Any) => fail("Unacceptable failure: " + e)
    }
    captureAllFailed.values should have length 4
  }

  private def genEvent(): ReconcileEvent = {
    new ReconcileEvent(
      new EventsMeta(now, evtId, domain, mystream, requestId),
      "https://schema.wikimedia.org/repositories/secondary/jsonschema" + ReconcileEvent.SCHEMA, "Q1", 123L,
      "source", Action.CREATION,
      new EventInfo(new EventsMeta(now, evtId, domain, mystream, requestId), "another_schema")
    )
  }
}
