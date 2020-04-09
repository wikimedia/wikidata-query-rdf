package org.wikidata.query.rdf.updater

import com.codahale.metrics.MetricRegistry
import org.apache.flink.metrics.MetricGroup
import org.openrdf.model.Statement
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris

import collection.JavaConverters._


case class WikibaseEntityRevRepository(uris: Uris, metricGroup: MetricGroup) extends WikibaseEntityRevRepositoryTrait {

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
