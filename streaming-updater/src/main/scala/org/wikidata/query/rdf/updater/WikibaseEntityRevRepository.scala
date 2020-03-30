package org.wikidata.query.rdf.updater

import org.apache.http.HttpHost
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.openrdf.model.Statement
import org.openrdf.rio.helpers.StatementCollector
import org.openrdf.rio.{RDFFormat, RDFParser, Rio}
import org.slf4j.{Logger, LoggerFactory}
import org.wikidata.query.rdf.tool.rdf.NormalizingRdfHandler
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris

import scala.collection.JavaConverters.iterableAsScalaIterableConverter


case class WikibaseEntityRevRepository(uris: Uris, proxyServer: Option[String] = None) extends WikibaseEntityRevRepositoryTrait {
//  lazy val uris: Uris = WikibaseRepository.Uris.fromString(s"https://$sourceHostname")
  lazy val parser: RDFParser = Rio.createParser(RDFFormat.TURTLE)
  lazy val client: CloseableHttpClient = {
    val builder: HttpClientBuilder = HttpClientBuilder.create()
    proxyServer
      .map(proxyHost => builder.setProxy(HttpHost.create(proxyHost)))
      .getOrElse(builder)
      .build()
  }

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  override def getEntityByRevision(entityId: String, revision: Long): Iterable[Statement] = {
    try {
      val response: CloseableHttpResponse = client.execute(new HttpGet(uris.rdf(entityId, revision)))
      response.getEntity.getContent
      val collector: StatementCollector = new StatementCollector();

      parser.setRDFHandler(new NormalizingRdfHandler(collector))
      parser.parse(response.getEntity.getContent, uris.getHost)
      collector.getStatements.asScala
    } catch {
      case e: Exception => {
        LOG.error(s"Exception for $entityId thrown.", e)
        Seq()
      }
    }
  }
}
