package org.wikidata.query.rdf.spark.analysis.visitors

import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.walker.Walker
import org.apache.jena.sparql.syntax._

import scala.collection.JavaConverters._

/**
 * Currently not used
 */
class AnalyzeElementVisitor(
  nodeVisitor: AnalyzeNodeVisitor,
  pathVisitor: AnalyzePathVisitor,
  exprVisitor: AnalyzeExprVisitor,
  opVisitor: AnalyzeOpVisitor
) extends ElementVisitor {

  override def visit(elementTriplesBlock: ElementTriplesBlock): Unit = {
    elementTriplesBlock.getPattern.getList.asScala.foreach(t => {
      t.getSubject.visitWith(nodeVisitor)
      t.getPredicate.visitWith(nodeVisitor)
      t.getObject.visitWith(nodeVisitor)
    })
  }

  override def visit(elementPathBlock: ElementPathBlock): Unit = {
    elementPathBlock.getPattern.getList.asScala.foreach(triplePath => {
      triplePath.getSubject.visitWith(nodeVisitor)
      triplePath.getObject.visitWith(nodeVisitor)
      if (triplePath.getPath != null) {
        triplePath.getPath.visit(pathVisitor)
      } else {
        triplePath.getPredicate.visitWith(nodeVisitor)
      }
    })
  }

  override def visit(elementFilter: ElementFilter): Unit = elementFilter.getExpr.visit(exprVisitor)

  override def visit(elementAssign: ElementAssign): Unit = {
    elementAssign.getVar.asNode().visitWith(nodeVisitor)
    elementAssign.getExpr.visit(exprVisitor)
  }

  override def visit(elementBind: ElementBind): Unit = {
    elementBind.getVar.asNode().visitWith(nodeVisitor)
    elementBind.getExpr.visit(exprVisitor)
  }

  override def visit(elementData: ElementData): Unit = {
    elementData.getTable.rows().asScala.foreach(b => {
      b.vars().asScala.foreach(v => {
        b.get(v).visitWith(nodeVisitor)
      })
    })
  }

  override def visit(elementUnion: ElementUnion): Unit = {
    elementUnion.getElements.asScala.foreach(_.visit(this))
  }

  override def visit(elementOptional: ElementOptional): Unit = {
    elementOptional.getOptionalElement.visit(this)
  }

  override def visit(elementGroup: ElementGroup): Unit = {
    elementGroup.getElements.asScala.foreach(_.visit(this))
  }

  // Don't know !!!
  override def visit(elementDataset: ElementDataset): Unit = ()

  override def visit(elementNamedGraph: ElementNamedGraph): Unit = {
    elementNamedGraph.getElement.visit(this)
    elementNamedGraph.getGraphNameNode.visitWith(nodeVisitor)
  }

  override def visit(elementExists: ElementExists): Unit = {
    elementExists.getElement.visit(this)
  }

  override def visit(elementNotExists: ElementNotExists): Unit = {
    elementNotExists.getElement.visit(this)
  }

  override def visit(elementMinus: ElementMinus): Unit = {
    elementMinus.getMinusElement.visit(this)
  }

  override def visit(elementService: ElementService): Unit = {
    elementService.getElement.visit(this)
    elementService.getServiceNode.visitWith(nodeVisitor)
  }

  override def visit(elementSubQuery: ElementSubQuery): Unit = {
    Walker.walk(Algebra.compile(elementSubQuery.getQuery), opVisitor)
  }
}
