package org.wikidata.query.rdf.updater

object EntityStatus extends Enumeration {
  val DELETED, CREATED, UNDEFINED = Value
}
case class State(lastRevision: Option[Long], entityStatus: EntityStatus.Value)
