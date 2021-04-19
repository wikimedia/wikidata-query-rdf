package org.wikidata.query.rdf.updater

import java.time.Duration

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import org.apache.flink.metrics.MetricGroup
import org.openrdf.model.Statement
import org.slf4j.{Logger, LoggerFactory}
import org.wikidata.query.rdf.tool.rdf.{RDFParserSupplier, RDFParserSuppliers}
import org.wikidata.query.rdf.tool.utils.NullStreamDumper
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris


case class WikibaseEntityRevRepository(uris: Uris, metricGroup: MetricGroup) extends WikibaseEntityRevRepositoryTrait {

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
    RDFParserSuppliers.defaultRdfParser())

  override def getEntityByRevision(entityId: String, revision: Long): Iterable[Statement] = {
    wikibaseRepository.fetchRdfForEntity(entityId, revision).asScala
  }
}
