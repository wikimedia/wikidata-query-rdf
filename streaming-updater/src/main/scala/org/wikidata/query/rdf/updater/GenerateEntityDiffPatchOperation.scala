package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util
import java.util.function.Supplier
import java.util.Collections.emptyList
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.util.Collector
import org.openrdf.model.Statement
import org.openrdf.rio.{RDFFormat, RDFWriterRegistry}
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.{UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.exception.ContainedException
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Munger, RDFPatch}
import org.wikidata.query.rdf.tool.stream.{MutationEventData, MutationEventDataGenerator, RDFChunkSerializer}

sealed trait ResolvedOp {
  val operation: MutationOperation
}

case class EntityPatchOp(operation: MutationOperation,
                         data: MutationEventData) extends ResolvedOp
case class FailedOp(operation: MutationOperation, error: String) extends ResolvedOp

case class GenerateEntityDiffPatchOperation(domain: String,
                                            wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                            mimeType: String = RDFFormat.TURTLE.getDefaultMIMEType,
                                            chunkSoftMaxSize: Int = 128000, // ~max 128k chars, can be slightly more
                                            clock: Clock = Clock.systemUTC(),
                                            uniqueIdGenerator: () => String = () => UUID.randomUUID().toString,
                                            stream: String = "wdqs_streaming_updater"
                                  )
  extends RichFlatMapFunction[MutationOperation, ResolvedOp] {


  private val LOG = LoggerFactory.getLogger(getClass)

  lazy val repository: WikibaseEntityRevRepositoryTrait =  wikibaseRepositoryGenerator(this.getRuntimeContext)
  lazy val scheme: UrisScheme = UrisSchemeFactory.forHost(domain)
  lazy val munger: Munger = Munger.builder(scheme).convertBNodesToSkolemIRIs(true).build()
  lazy val diff: EntityDiff = EntityDiff.withValuesAndRefsAsSharedElements(scheme)
  lazy val rdfSerializer: RDFChunkSerializer = new RDFChunkSerializer(RDFWriterRegistry.getInstance())
  lazy val dataEventGenerator: MutationEventDataGenerator = new MutationEventDataGenerator(rdfSerializer, mimeType, chunkSoftMaxSize)


  override def flatMap(op: MutationOperation, collector: Collector[ResolvedOp]): Unit =  try {
    // TODO: find a way to convert scala lambdas () => Type as a java Supplier[Type]
    val eventMetaSupplier: Supplier[EventsMeta] = new Supplier[EventsMeta] {
      override def get(): EventsMeta = new EventsMeta(clock.instant(), uniqueIdGenerator.apply(), domain, stream, "TODO")
    }
    val events = op match {
      case Diff(item, _, revision, fromRev, _) =>
        val patch = diff(item, repository.getEntityByRevision(item, fromRev), repository.getEntityByRevision(item, revision))
        dataEventGenerator.diffEvent(eventMetaSupplier, item, revision, op.eventTime, patch.getAdded, patch.getRemoved,
          patch.getLinkedSharedElements, patch.getUnlinkedSharedElements)
      case FullImport(item, _, revision, _) =>
        val patch = fullImport(item, repository.getEntityByRevision(item, revision));
        dataEventGenerator.fullImportEvent(eventMetaSupplier, item, revision, op.eventTime, patch.getAdded, patch.getLinkedSharedElements)
    }
    events.asScala.foreach {e => collector.collect(EntityPatchOp(op, e))}
  } catch {
    case exception: ContainedException => {
      LOG.error(s"Exception thrown for op: $op", exception)
      collector.collect(FailedOp(op, exception.toString))
    }
  }

  def diff(item: String, from: Iterable[Statement], to: Iterable[Statement]): RDFPatch = {
    val fromList = new util.ArrayList[Statement](from.asJavaCollection)
    val toList = new util.ArrayList[Statement](to.asJavaCollection)

    munger.munge(item, fromList)
    munger.munge(item, toList)

    diff.diff(fromList, toList)
  }

  def fullImport(item: String, stmts: Iterable[Statement]): RDFPatch = {
    val toList = new util.ArrayList[Statement](stmts.asJavaCollection)

    munger.munge(item, toList)

    diff.diff(emptyList(), toList)
  }
}

sealed class RouteFailedOpsToSideOutput(ignoredEventTag: OutputTag[FailedOp] = RouteFailedOpsToSideOutput.FAILED_OPS_TAG)
  extends ProcessFunction[ResolvedOp, EntityPatchOp]
{
  override def processElement(i: ResolvedOp,
                              context: ProcessFunction[ResolvedOp, EntityPatchOp]#Context,
                              collector: Collector[EntityPatchOp]
                             ): Unit = {
    i match {
      case e: FailedOp => context.output(ignoredEventTag, e)
      case x: EntityPatchOp => collector.collect(x)
    }
  }
}

object RouteFailedOpsToSideOutput {
  val FAILED_OPS_TAG: OutputTag[FailedOp] = new OutputTag[FailedOp]("failed-ops-events")
}
