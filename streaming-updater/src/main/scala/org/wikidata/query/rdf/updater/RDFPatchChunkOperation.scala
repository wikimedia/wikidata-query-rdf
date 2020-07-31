package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util
import java.util.UUID
import java.util.function.Supplier

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.openrdf.rio.{RDFFormat, RDFWriterRegistry}
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.stream.{DiffEventData, MutationEventData, MutationEventDataGenerator, RDFChunkSerializer}

sealed case class MutationDataChunk(
                                     operation: MutationOperation,
                                     data: MutationEventData
)

class RDFPatchChunkOperation(domain: String,
                             mimeType: String = RDFFormat.TURTLE.getDefaultMIMEType,
                             chunkSoftMaxSize: Int = 128000, // ~max 128k chars, can be slightly more
                             clock: Clock = Clock.systemUTC(),
                             uniqueIdGenerator: () => String = () => UUID.randomUUID().toString,
                             stream: String = "wdqs_streaming_updater"
                            )
  extends FlatMapFunction[EntityPatchOp, MutationDataChunk] {

  lazy val rdfSerializer: RDFChunkSerializer = new RDFChunkSerializer(RDFWriterRegistry.getInstance())
  lazy val dataEventGenerator: MutationEventDataGenerator = new MutationEventDataGenerator(rdfSerializer, mimeType, chunkSoftMaxSize)

  override def flatMap(t: EntityPatchOp, collector: Collector[MutationDataChunk]): Unit = {
    val dataList: util.List[DiffEventData] = t.operation match {
      case FullImport(entity, eventTime, rev, _, originalEventMetadata) =>
        dataEventGenerator.fullImportEvent(eventMetaSupplier(originalEventMetadata), entity, rev, eventTime, t.data.getAdded, t.data.getLinkedSharedElements)
      case Diff(entity, eventTime, rev, _, _, originalEventMetadata) =>
        dataEventGenerator.diffEvent(eventMetaSupplier(originalEventMetadata), entity, rev, eventTime, t.data.getAdded,
          t.data.getRemoved, t.data.getLinkedSharedElements, t.data.getUnlinkedSharedElements)
    }
    dataList.asScala map {MutationDataChunk(t.operation, _)} foreach collector.collect
  }


  private def eventMetaSupplier(originalEventMeta: EventsMeta): Supplier[EventsMeta] = {
    new Supplier[EventsMeta] {
      override def get(): EventsMeta = new EventsMeta(clock.instant(), uniqueIdGenerator.apply(), domain, stream, originalEventMeta.requestId())
    }
  }
}
