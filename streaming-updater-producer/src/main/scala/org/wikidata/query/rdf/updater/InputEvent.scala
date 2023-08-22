package org.wikidata.query.rdf.updater

import java.time.Instant

import org.wikidata.query.rdf.tool.change.events.EventInfo
import org.wikidata.query.rdf.tool.EntityId

trait BasicEventData {
  val item: String
  val eventTime: Instant
  val revision: Long
  val ingestionTime: Instant
  val originalEventInfo: EventInfo
}

sealed trait InputEvent extends BasicEventData

/** Describe a new revision */
final case class RevCreate(item: String,
                           eventTime: Instant,
                           revision: Long,
                           parentRevision: Option[Long],
                           ingestionTime: Instant,
                           originalEventInfo: EventInfo
                    ) extends InputEvent

object RevCreate {
  def apply(item: EntityId,
            eventTime: Instant,
            revision: Long,
            parentRevision: Option[Long],
            ingestionTime: Instant,
            originalEventInfo: EventInfo): RevCreate =
    this(item.toString, eventTime, revision, parentRevision, ingestionTime, originalEventInfo)
}

final case class ReconcileInputEvent(item: String,
                                     eventTime: Instant,
                                     revision: Long,
                                     originalAction: ReconcileOriginalAction,
                                     ingestionTime: Instant,
                                     originalEventInfo: EventInfo
                               ) extends InputEvent

object ReconcileInputEvent {
  def apply(item: EntityId,
            eventTime: Instant,
            revision: Long,
            originalAction: ReconcileOriginalAction,
            ingestionTime: Instant,
            originalEventInfo: EventInfo): ReconcileInputEvent =
    this(item.toString, eventTime, revision, originalAction, ingestionTime, originalEventInfo)
}

sealed trait ReconcileOriginalAction
case object ReconcileCreation extends ReconcileOriginalAction
case object ReconcileDeletion extends ReconcileOriginalAction

/** Describe a delete event */
final case class PageDelete(item: String,
                            eventTime: Instant,
                            revision: Long,
                            ingestionTime: Instant,
                            originalEventInfo: EventInfo
                       ) extends InputEvent

object PageDelete {
    def apply(item: EntityId,
              eventTime: Instant,
              revision: Long,
              ingestionTime: Instant,
              originalEventInfo: EventInfo): PageDelete =
      this(item.toString, eventTime, revision, ingestionTime, originalEventInfo)
}

/** Describe an undelete event */
final case class PageUndelete(item: String,
                              eventTime: Instant,
                              revision: Long,
                              ingestionTime: Instant,
                              originalEventInfo: EventInfo
                           ) extends InputEvent

object PageUndelete {
  def apply(item: EntityId,
            eventTime: Instant,
            revision: Long,
            ingestionTime: Instant,
            originalEventInfo: EventInfo): PageUndelete =
    this(item.toString, eventTime, revision, ingestionTime, originalEventInfo)
}

