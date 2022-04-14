package org.wikidata.query.rdf.spark.transform.queries.sparql.visitors

import org.apache.jena.sparql.path._

import scala.collection.mutable
import collection.JavaConverters._

class AnalyzePathVisitor(nodeVisitor: AnalyzeNodeVisitor) extends PathVisitor {

  val pathVisited: mutable.Map[String, Long] =new mutable.HashMap[String, Long]().withDefaultValue(0L)
  private def incPathVisited(p: Path) = pathVisited(p.toString) = pathVisited(p.toString) + 1L

  override def visit(pLink: P_Link): Unit = {
    incPathVisited(pLink)
    pLink.getNode.visitWith(nodeVisitor)
  }

  override def visit(pReverseLink: P_ReverseLink): Unit = {
    incPathVisited(pReverseLink)
    pReverseLink.getNode.visitWith(nodeVisitor)
  }

  override def visit(pNegPropSet: P_NegPropSet): Unit = {
    incPathVisited(pNegPropSet)
    pNegPropSet.getNodes.asScala.foreach(p0 => p0.getNode.visitWith(nodeVisitor))
  }

  override def visit(pInverse: P_Inverse): Unit = {
    incPathVisited(pInverse)
    pInverse.getSubPath.visit(this)
  }

  override def visit(pMod: P_Mod): Unit = {
    incPathVisited(pMod)
    pMod.getSubPath.visit(this)
  }

  override def visit(pFixedLength: P_FixedLength): Unit = {
    incPathVisited(pFixedLength)
    pFixedLength.getSubPath.visit(this)
  }

  override def visit(pDistinct: P_Distinct): Unit = {
    incPathVisited(pDistinct)
    pDistinct.getSubPath.visit(this)
  }

  override def visit(pMulti: P_Multi): Unit = {
    incPathVisited(pMulti)
    pMulti.getSubPath.visit(this)
  }

  override def visit(pShortest: P_Shortest): Unit = {
    incPathVisited(pShortest)
    pShortest.getSubPath.visit(this)
  }

  override def visit(pZeroOrOne: P_ZeroOrOne): Unit = {
    incPathVisited(pZeroOrOne)
    pZeroOrOne.getSubPath.visit(this)
  }

  override def visit(pZeroOrMore1: P_ZeroOrMore1): Unit = {
    incPathVisited(pZeroOrMore1)
    pZeroOrMore1.getSubPath.visit(this)
  }

  override def visit(pZeroOrMoreN: P_ZeroOrMoreN): Unit = {
    incPathVisited(pZeroOrMoreN)
    pZeroOrMoreN.getSubPath.visit(this)
  }

  override def visit(pOneOrMore1: P_OneOrMore1): Unit = {
    incPathVisited(pOneOrMore1)
    pOneOrMore1.getSubPath.visit(this)
  }

  override def visit(pOneOrMoreN: P_OneOrMoreN): Unit = {
    incPathVisited(pOneOrMoreN)
    pOneOrMoreN.getSubPath.visit(this)
  }

  override def visit(pAlt: P_Alt): Unit = {
    incPathVisited(pAlt)
    pAlt.getLeft.visit(this)
    pAlt.getRight.visit(this)
  }

  override def visit(pSeq: P_Seq): Unit = {
    incPathVisited(pSeq)
    pSeq.getLeft.visit(this)
    pSeq.getRight.visit(this)
  }
}

