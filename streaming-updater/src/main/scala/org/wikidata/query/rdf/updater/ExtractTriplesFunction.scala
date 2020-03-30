package org.wikidata.query.rdf.updater

import java.util

import org.apache.flink.api.common.functions.MapFunction
import org.openrdf.model.Statement

import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.tool.rdf.Munger

import scala.collection.JavaConverters._

case class EntityTripleDiffs(operation: MutationOperation, adds: Set[Statement], removes: Set[Statement] = Set())

case class ExtractTriplesFunction(domain: String, client: WikibaseEntityRevRepositoryTrait) extends MapFunction[MutationOperation, EntityTripleDiffs] {

  override def map(op: MutationOperation): EntityTripleDiffs =  op match {
      case Diff(item, _, revision, fromRev) =>
        val revStatements: Set[Statement] = munge(item, client.getEntityByRevision(item, revision))
        val fromRevStatements: Set[Statement] = munge(item, client.getEntityByRevision(item, fromRev))
        EntityTripleDiffs(op,
          revStatements -- fromRevStatements, fromRevStatements -- revStatements)

      case FullImport(item, _, revision) =>
        EntityTripleDiffs(op, munge(item, client.getEntityByRevision(item, revision)))
  }

  def munge(item: String, st: Iterable[Statement]): Set[Statement] = {

    val munger: Munger = Munger.builder(UrisSchemeFactory.forHost(domain)).build()

    val mutableList = new util.ArrayList[Statement]()
    mutableList.addAll(st.asJavaCollection)
    munger.munge(item, mutableList)
    mutableList.asScala.toSet
  }
}
