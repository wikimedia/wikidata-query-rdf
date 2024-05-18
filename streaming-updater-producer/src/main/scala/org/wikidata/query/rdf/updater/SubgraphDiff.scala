package org.wikidata.query.rdf.updater

import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.model.{Statement, URI, ValueFactory}
import org.wikidata.query.rdf.common.uri.{Ontology, UrisScheme}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Patch}

import java.util
import java.util.Collections.emptyList
import scala.collection.JavaConverters.{asJavaCollectionConverter, asJavaIterableConverter}

class SubgraphDiff(entityDiff: EntityDiff, subgraphAssigner: SubgraphAssigner, scheme: UrisScheme) extends Serializable {

  @transient private lazy val valueFactory: ValueFactory = new ValueFactoryImpl()

  private val subgraphPredicate = valueFactory.createURI(Ontology.QueryService.SUBGRAPH)

  private def stubFor(entityUri: URI, containingSubgraphUri: URI): Statement = {
    assert(entityUri != null, "entity URI must not be null")
    assert(containingSubgraphUri != null, "s(t)ubgraph URI must not be null")
    valueFactory.createStatement(entityUri, subgraphPredicate, containingSubgraphUri)
  }

  // scalastyle:off method.length
  def diff(implicit operation: Diff, entityFrom: Iterable[Statement], entityTo: Iterable[Statement]): Iterable[SuccessfulOp] = {
    val entityUri: URI = entityURI(operation.item)

    val fromSubgraphs = subgraphAssigner.assign(entityFrom)
    val toSubgraphs = subgraphAssigner.assign(entityTo)
    val unchangedSubgraphs = fromSubgraphs
      .filter(fromSubgraph => toSubgraphs.contains(fromSubgraph))

    val leftSubgraphs = fromSubgraphs
      .filter(fromSubgraph => fromSubgraph.getSubgraphUri != null)
      .filterNot(fromSubgraph => toSubgraphs.contains(fromSubgraph))
    val enteredSubgraphs = toSubgraphs
      .filter(toSubgraph => toSubgraph.getSubgraphUri != null)
      .filterNot(toSubgraph => fromSubgraphs.contains(toSubgraph))

    val unassignedSubgraphs = subgraphAssigner.distinctUnassignedSubgraphsFor((fromSubgraphs ++ toSubgraphs).distinct)

    val fullPatch = entityDiff.diff(entityFrom.asJava, entityTo.asJava)

    val ops = List.newBuilder[SuccessfulOp]

    // forward full diff to all unchanged subgraphs (original behaviour)
    ops ++= unchangedSubgraphs
      .map(unchangedSubgraph => EntityPatchOp(operation, fullPatch, Option(unchangedSubgraph.getSubgraphUri)))

    // clean up: use DeleteOp to remove all triples (including stubs) from all subgraphs
    ops ++= leftSubgraphs.map(leftSubgraph => DeleteOp(operation, Option(leftSubgraph.getSubgraphUri)))

    val leftStubSourceSubgraph = leftSubgraphs.exists(SubgraphAssigner.isDistinctStubSource)

    if (leftStubSourceSubgraph) {
      ops ++= unassignedSubgraphs.map(unassignedSubgraph => DeleteOp(operation, Option(unassignedSubgraph.getSubgraphUri)))
    }

    ops ++= enteredSubgraphs
      .flatMap(enteredSubgraph => Seq(
        DeleteOp(operation, Option(enteredSubgraph.getSubgraphUri)),
        EntityPatchOp(operation, entityDiff.diff(emptyList(), entityTo.asJava), Some(enteredSubgraph.getSubgraphUri))
      ))

    // stubs: if any subgraph was entered patch those left and unassigned
    val enteredAndUnchangedStubSourceSubgraphs = subgraphAssigner.distinctStubSourcesFor(enteredSubgraphs ++ unchangedSubgraphs)
    if (enteredAndUnchangedStubSourceSubgraphs.nonEmpty) {
      val stubPatch = stubPatchFor(
        entityUri,
        enteredAndUnchangedStubSourceSubgraphs.map(_.getSubgraphUri))

      ops ++= (leftSubgraphs ++ unassignedSubgraphs)
        .map(leftSubgraph => EntityPatchOp(operation, stubPatch, Option(leftSubgraph.getSubgraphUri)))
    }

    ops.result()
  }
  // scalastyle:on method.length

  def fullImport(implicit operation: FullImport, entity: Iterable[Statement]): Iterable[SuccessfulOp] = {
    val assignedSubgraphs = subgraphAssigner.assign(entity)
    val addPatch = entityDiff.diff(emptyList(), entity.asJava)

    val ops = List.newBuilder[SuccessfulOp]

    ops ++= assignedSubgraphs
      .map(assignedSubgraph => EntityPatchOp(operation, addPatch, Option(assignedSubgraph.getSubgraphUri)))

    val stubSourceSubgraphs = subgraphAssigner.distinctStubSourcesFor(assignedSubgraphs)

    if (stubSourceSubgraphs.nonEmpty) {
      val unassignedSubgraphs = subgraphAssigner.distinctUnassignedSubgraphsFor(assignedSubgraphs)
      val stubPatch = stubPatchFor(entityURI(operation.item), stubSourceSubgraphs.map(_.getSubgraphUri))

      ops ++= unassignedSubgraphs
        .map(unassignedSubgraph => EntityPatchOp(operation, stubPatch, Option(unassignedSubgraph.getSubgraphUri)))
    }

    ops.result()
  }

  def reconcile(implicit operation: Reconcile, entity: Iterable[Statement]): Iterable[SuccessfulOp] = {
    val assignedSubgraphs = subgraphAssigner.assign(entity)
    val addPatch = entityDiff.diff(emptyList(), entity.asJava)

    val ops = List.newBuilder[SuccessfulOp]

    ops ++= assignedSubgraphs
      .map(assignedSubgraph => ReconcileOp(operation, addPatch, Option(assignedSubgraph.getSubgraphUri)))

    val unassignedSubgraphs = subgraphAssigner.distinctUnassignedSubgraphsFor(assignedSubgraphs)

    val stubSourceSubgraphs = subgraphAssigner.distinctStubSourcesFor(assignedSubgraphs)

    if (stubSourceSubgraphs.nonEmpty) {
      val stubPatch = stubPatchFor(entityURI(operation.item), stubSourceSubgraphs.map(_.getSubgraphUri))

      ops ++= unassignedSubgraphs
        .map(unassignedSubgraph => ReconcileOp(operation, stubPatch, Some(unassignedSubgraph.getSubgraphUri)))
    } else {
      val noStubSourceSubgraphs = assignedSubgraphs
        .filter(SubgraphAssigner.isDistinct)
        .forall(subgraphDefinition => !subgraphDefinition.isStubsSource)

      if (noStubSourceSubgraphs) {
        ops ++= unassignedSubgraphs
          .map(unassignedSubgraph => DeleteOp(operation, Some(unassignedSubgraph.getSubgraphUri)))
      }
    }

    ops.result()
  }

  def delete(implicit operation: DeleteItem): Iterable[SuccessfulOp] = {
    val ops = List.newBuilder[SuccessfulOp]

    ops += DeleteOp(operation)

    ops ++= subgraphAssigner.distinctSubgraphs.map(subgraphUri => {
      DeleteOp(operation, Option(subgraphUri.getSubgraphUri))
    })

    ops.result()
  }

  private def entityURI(entityId: String) = valueFactory.createURI(scheme.entityIdToURI(entityId))

  private def stubPatchFor(entity: URI, enteredAndUnchangedStubSourceUris: Iterable[URI]): Patch = {
    val linkedSharedElements = enteredAndUnchangedStubSourceUris.map(stubSourceUri => stubFor(entity, stubSourceUri))
    new Patch(
      emptyList(),
      new util.ArrayList[Statement](linkedSharedElements.asJavaCollection),
      emptyList(),
      emptyList())
  }

}
