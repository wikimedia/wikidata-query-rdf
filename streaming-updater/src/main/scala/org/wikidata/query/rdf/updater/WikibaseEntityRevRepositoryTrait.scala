package org.wikidata.query.rdf.updater

import org.openrdf.model.Statement

trait WikibaseEntityRevRepositoryTrait {

  def getEntityByRevision(entityId: String, revision: Long): Iterable[Statement]
}
