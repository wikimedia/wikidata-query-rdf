package org.wikidata.query.rdf.updater

import java.util

import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.openrdf.model.Statement
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.tool.exception.ContainedException
import org.wikidata.query.rdf.tool.rdf.Munger

import scala.collection.JavaConverters._

sealed trait ResolvedOp {
  val operation: MutationOperation
}

case class EntityTripleDiffs(operation: MutationOperation, adds: Set[Statement], removes: Set[Statement] = Set()) extends ResolvedOp
case class FailedOp(operation: MutationOperation) extends ResolvedOp

case class ExtractTriplesOperation(domain: String, wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait)
  extends RichMapFunction[MutationOperation, ResolvedOp] {

  val LOG = LoggerFactory.getLogger(getClass)
  lazy val repository: WikibaseEntityRevRepositoryTrait =  wikibaseRepositoryGenerator(this.getRuntimeContext)

  override def map(op: MutationOperation): ResolvedOp = {
    try {
      op match {
        case Diff(item, _, revision, fromRev, _) =>
          val revStatements: Set[Statement] = munge(item, repository.getEntityByRevision(item, revision))
          val fromRevStatements: Set[Statement] = munge(item, repository.getEntityByRevision(item, fromRev))
          EntityTripleDiffs(op,
            revStatements -- fromRevStatements, fromRevStatements -- revStatements)

        case FullImport(item, _, revision, _) =>
          EntityTripleDiffs(op, munge(item, repository.getEntityByRevision(item, revision)))
      }
    } catch {
      case exception: ContainedException => {
        LOG.error(s"Exception thrown for op: $op", exception)
        FailedOp(op)
      }
    }
  }

  def munge(item: String, st: Iterable[Statement]): Set[Statement] = {

    val munger: Munger = Munger.builder(UrisSchemeFactory.forHost(domain)).build()

    val mutableList = new util.ArrayList[Statement]()
    mutableList.addAll(st.asJavaCollection)
    munger.munge(item, mutableList)
    mutableList.asScala.toSet
  }
}
