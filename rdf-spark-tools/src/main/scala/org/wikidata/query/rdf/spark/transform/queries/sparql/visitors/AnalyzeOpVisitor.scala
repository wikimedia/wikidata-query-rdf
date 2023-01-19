package org.wikidata.query.rdf.spark.transform.queries.sparql.visitors

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.algebra.{Op, OpVisitor}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.core.Var

case class TripleInfo(subjectNode: NodeInfo, predicateNode: NodeInfo, objectNode: NodeInfo)

// Number of methods is defined by parent class - turning off scalastyle check
// scalastyle:off number.of.methods
class AnalyzeOpVisitor(
  prefixMapping: PrefixMapping
) extends OpVisitor {

  var tripleGlobalCount = 0L
  var triplePathCount = 0L
  val opCount: mutable.Map[String, Long] = new mutable.HashMap[String, Long]().withDefaultValue(0L)
  val opList: mutable.Buffer[String] = new mutable.ArrayBuffer[String]()
  val triples: mutable.Buffer[TripleInfo] = new mutable.ArrayBuffer[TripleInfo]()

  val nodeVisitor = new AnalyzeNodeVisitor(prefixMapping)
  val serviceVisitor = new AnalyzeNodeVisitor(prefixMapping)
  val pathVisitor = new AnalyzePathVisitor(nodeVisitor)
  val exprVisitor = new AnalyzeExprVisitor(nodeVisitor, /*pathVisitor, */this)

  private def inc(s: String): Unit = {
    opCount(s) = opCount(s) + 1L
    opList += s
  }

  private def incTriple(s: TripleInfo): Unit = {
    triples += s
  }

  private def notWorked(op: Op): Unit ={
    val opName = op.getName
    opCount(opName) = opCount(opName) + 1L
  }

  private def tbd(op: Op): Unit ={
    inc(op.getName)
  }

  override def visit(opBGP: OpBGP): Unit = {
    inc(opBGP.getName)
    opBGP.getPattern.getList.asScala.foreach(t => {
      tripleGlobalCount += 1
      val subNode = t.getSubject.visitWith(nodeVisitor).asInstanceOf[NodeInfo]
      val predNode = t.getPredicate.visitWith(nodeVisitor).asInstanceOf[NodeInfo]
      val objNode = t.getObject.visitWith(nodeVisitor).asInstanceOf[NodeInfo]
      incTriple(TripleInfo(subNode, predNode, objNode))
    })
  }

  override def visit(opService: OpService): Unit = {
    inc(opService.getName)
    opService.getService.visitWith(serviceVisitor)
  }

  override def visit(opTable: OpTable): Unit = {
    inc(opTable.getName)
    opTable.getTable.rows().asScala.foldLeft(Set.empty[Var])((seen, b) => {
      b.vars().asScala.foldLeft(seen)((seen2, v) => {
        if (! seen2.contains(v)) {
          v.visitWith(nodeVisitor)
        }
        b.get(v).visitWith(nodeVisitor)
        seen2 + v
      })
    })
    ()
  }

  override def visit(opPath: OpPath): Unit = {
    inc(opPath.getName)
    val triplePath = opPath.getTriplePath
    val subNode = triplePath.getSubject.visitWith(nodeVisitor).asInstanceOf[NodeInfo]
    val objNode = triplePath.getObject.visitWith(nodeVisitor).asInstanceOf[NodeInfo]
    if (triplePath.getPath != null) {
      triplePathCount += 1
      triplePath.getPath.visit(pathVisitor)
      val predNode = NodeInfo("PATH", triplePath.getPath.toString())
      incTriple(TripleInfo(subNode, predNode, objNode))
    } else {
      tripleGlobalCount += 1
      val predNode = triplePath.getPredicate.visitWith(nodeVisitor).asInstanceOf[NodeInfo]
      incTriple(TripleInfo(subNode, predNode, objNode))
    }
  }

  override def visit(opFilter: OpFilter): Unit = {
    inc(opFilter.getName)
    opFilter.getExprs.getList.asScala.foreach(_.visit(exprVisitor))
  }

  override def visit(opNull: OpNull): Unit = tbd(opNull)

  override def visit(opGraph: OpGraph): Unit = tbd(opGraph)

  override def visit(opLabel: OpLabel): Unit = tbd(opLabel)

  override def visit(opAssign: OpAssign): Unit = tbd(opAssign)

  override def visit(opExtend: OpExtend): Unit = {
    inc(opExtend.getName)
    val varExprList = opExtend.getVarExprList
    varExprList.getVars.asScala.foldLeft(Set.empty[Var])((seen, v) => {
      if (! seen.contains(v)) {
        v.visitWith(nodeVisitor)
      }
      varExprList.getExpr(v).visit(exprVisitor)
      seen + v
    })
    ()
  }

  override def visit(opJoin: OpJoin): Unit = tbd(opJoin)

  override def visit(opLeftJoin: OpLeftJoin): Unit = tbd(opLeftJoin)

  override def visit(opUnion: OpUnion): Unit = tbd(opUnion)

  override def visit(opDiff: OpDiff): Unit = tbd(opDiff)

  override def visit(opMinus: OpMinus): Unit = tbd(opMinus)

  override def visit(opConditional: OpConditional): Unit = tbd(opConditional)

  override def visit(opSequence: OpSequence): Unit = tbd(opSequence)

  override def visit(opDisjunction: OpDisjunction): Unit = tbd(opDisjunction)

  override def visit(opList: OpList): Unit = tbd(opList)

  override def visit(opOrder: OpOrder): Unit = tbd(opOrder)

  override def visit(opProject: OpProject): Unit = tbd(opProject)

  override def visit(opReduced: OpReduced): Unit = tbd(opReduced)

  override def visit(opDistinct: OpDistinct): Unit = tbd(opDistinct)

  override def visit(opSlice: OpSlice): Unit = tbd(opSlice)

  override def visit(opGroup: OpGroup): Unit = tbd(opGroup)

  override def visit(opTopN: OpTopN): Unit = tbd(opTopN)

  /*
  NOT WORKED
  */

  override def visit(opQuadPattern: OpQuadPattern): Unit = notWorked(opQuadPattern)

  override def visit(opQuadBlock: OpQuadBlock): Unit = notWorked(opQuadBlock)

  override def visit(opTriple: OpTriple): Unit = notWorked(opTriple)

  override def visit(opQuad: OpQuad): Unit = notWorked(opQuad)

  override def visit(opProcedure: OpProcedure): Unit = notWorked(opProcedure)

  override def visit(opPropFunc: OpPropFunc): Unit = notWorked(opPropFunc)

  override def visit(opDatasetNames: OpDatasetNames): Unit = notWorked(opDatasetNames)

}
// scalastyle:on number.of.methods
