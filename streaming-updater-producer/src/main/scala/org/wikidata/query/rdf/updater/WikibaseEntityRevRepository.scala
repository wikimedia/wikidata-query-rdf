package org.wikidata.query.rdf.updater

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import org.apache.flink.metrics.MetricGroup
import org.openrdf.model.Statement
import org.slf4j.{Logger, LoggerFactory}
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris


case class WikibaseEntityRevRepository(uris: Uris, metricGroup: MetricGroup) extends WikibaseEntityRevRepositoryTrait {

  val LOG: Logger = LoggerFactory.getLogger(WikibaseEntityRevRepository.getClass);
  private lazy val registry = {
    val metricRegistry = new MetricRegistry()
    metricRegistry.addListener(new DropwizardToFlinkListener(metricGroup))
    metricRegistry
  }
  lazy val wikibaseRepository: WikibaseRepository = new WikibaseRepository(uris, registry)

  override def getEntityByRevision(entityId: String, revision: Long): Iterable[Statement] = {
    wikibaseRepository.fetchRdfForEntity(entityId, revision).asScala
  }
}
