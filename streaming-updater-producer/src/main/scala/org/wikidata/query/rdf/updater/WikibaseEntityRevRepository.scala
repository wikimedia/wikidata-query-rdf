package org.wikidata.query.rdf.updater

import java.time.Duration

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import org.apache.flink.metrics.MetricGroup
import org.openrdf.model.Statement
import org.slf4j.{Logger, LoggerFactory}
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers
import org.wikidata.query.rdf.tool.utils.NullStreamDumper
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.updater.config.HttpClientConfig


case class WikibaseEntityRevRepository(uris: Uris, httpClientConfig: HttpClientConfig, metricGroup: MetricGroup) extends WikibaseEntityRevRepositoryTrait {

  val LOG: Logger = LoggerFactory.getLogger(WikibaseEntityRevRepository.getClass)
  private lazy val registry = {
    val metricRegistry = new MetricRegistry()
    metricRegistry.addListener(new DropwizardToFlinkListener(metricGroup))
    metricRegistry
  }
  lazy val wikibaseRepository: WikibaseRepository = new WikibaseRepository(uris,
    false,
    registry,
    new NullStreamDumper(),
    Duration.ofMillis(0), // unused here
    RDFParserSuppliers.defaultRdfParser(),
    HttpClientUtils.createHttpClient(
      HttpClientUtils.createConnectionManager(registry, timeout),
      None.orNull,
      httpClientConfig.httpRoutes.orNull,
      timeout,
      httpClientConfig.userAgent)
  )

  private def timeout: Int = {
    httpClientConfig.httpTimeout.getOrElse(HttpClientUtils.TIMEOUT.toMillis.intValue())
  }

  override def getEntityByRevision(entityId: String, revision: Long): Iterable[Statement] = {
    wikibaseRepository.fetchRdfForEntity(entityId, revision).asScala
  }
}
