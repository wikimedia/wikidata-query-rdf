package org.wikidata.query.rdf.updater

import org.openrdf.model.Statement

case class MockWikibaseEntityRevRepository (private val responses: Map[(String, Long), Iterable[Statement]] = Map())
  extends WikibaseEntityRevRepositoryTrait {
  def withResponse(response: ((String, Long), Iterable[Statement])): MockWikibaseEntityRevRepository =
    MockWikibaseEntityRevRepository(this.responses + response)

  override def getEntityByRevision(entityId: String, revision: Long): Iterable[Statement] = responses((entityId, revision))
}


