package org.wikidata.query.rdf.spark.analysis.visitors

import org.apache.jena.sparql.expr._
import scala.collection.mutable
import collection.JavaConverters._


class AnalyzeExprVisitor(
  nodeVisitor: AnalyzeNodeVisitor,
  //pathVisitor: AnalyzePathVisitor,
  opVisitor: AnalyzeOpVisitor
) extends ExprVisitor {

  val exprVisited: mutable.Map[String, Long] = new mutable.HashMap[String, Long]().withDefaultValue(0L)
  private def incExprVisited(e: Expr) = exprVisited(e.toString) = exprVisited(e.toString) + 1L

  override def visit(exprFunction0: ExprFunction0): Unit = {
    incExprVisited(exprFunction0)
    ()
  }

  override def visit(exprFunction1: ExprFunction1): Unit = {
    incExprVisited(exprFunction1)
    exprFunction1.getArg.visit(this)
  }

  override def visit(exprFunction2: ExprFunction2): Unit = {
    incExprVisited(exprFunction2)
    exprFunction2.getArg1.visit(this)
    exprFunction2.getArg2.visit(this)
  }

  override def visit(exprFunction3: ExprFunction3): Unit = {
    incExprVisited(exprFunction3)
    exprFunction3.getArg1.visit(this)
    exprFunction3.getArg2.visit(this)
    exprFunction3.getArg3.visit(this)
  }

  override def visit(exprFunctionN: ExprFunctionN): Unit = {
    incExprVisited(exprFunctionN)
    exprFunctionN.getArgs.asScala.foreach(_.visit(this))
  }

  override def visit(exprFunctionOp: ExprFunctionOp): Unit = {
    incExprVisited(exprFunctionOp)
    // Not used: exprFunctionOp.getElement.visit(new AnalyzeElementVisitor(nodeVisitor, pathVisitor, this, opVisitor))
    exprFunctionOp.getGraphPattern.visit(opVisitor)
  }

  override def visit(nodeValue: NodeValue): Unit = {
    nodeValue.asNode().visitWith(nodeVisitor)
  }

  override def visit(exprVar: ExprVar): Unit = {
    exprVar.getAsNode.visitWith(nodeVisitor)
  }

  override def visit(exprAggregator: ExprAggregator): Unit = {
    incExprVisited(exprAggregator)
    exprAggregator.getAggVar.getAsNode.visitWith(nodeVisitor)
  }

  override def visit(exprNone: ExprNone): Unit = {
    incExprVisited(exprNone)
    ()
  }
}
