package org.wikidata.query.rdf.spark.transform.queries.sparql.visitors

import org.apache.jena.graph.impl.LiteralLabel
import org.apache.jena.graph._
import org.apache.jena.shared.PrefixMapping

import scala.collection.mutable

case class NodeInfo(nodeType: String, nodeValue: String)

class AnalyzeNodeVisitor(
  prefixMapping: PrefixMapping
) extends NodeVisitor {

  val wdNodeCount: mutable.Map[String, Long] = new mutable.HashMap[String, Long]().withDefaultValue(0L)
  val nodeCount: mutable.Map[String, Long] = new mutable.HashMap[String, Long]().withDefaultValue(0L)
  val prefixesCount: mutable.Map[String, Long] = new mutable.HashMap[String, Long]().withDefaultValue(0L)

  val nullRes = Option.empty

  private def incWdNode(s: String): Unit = {
    wdNodeCount(s) = wdNodeCount(s) + 1L
  }

  private def incNode(s: String): Unit ={
    nodeCount(s) = nodeCount(s) + 1L
  }

  private def incPrefix(s: String): Unit = {
    prefixesCount(s) = prefixesCount(s) + 1L
  }

  override def visitAny(nodeAny: Node_ANY): NodeInfo = {
    incNode("NODE_ANY")
    NodeInfo("NODE_ANY", "")
  }

  override def visitBlank(nodeBlank: Node_Blank, blankNodeId: BlankNodeId): NodeInfo = {
    incNode(s"NODE_BLANK[${blankNodeId.getLabelString}]")
    NodeInfo("NODE_BLANK", blankNodeId.getLabelString)
  }

  override def visitLiteral(nodeLiteral: Node_Literal, literalLabel: LiteralLabel): NodeInfo = {
    incNode(s"NODE_LITERAL[${literalLabel.toString()}]")
    NodeInfo("NODE_LITERAL", literalLabel.toString())
  }

  override def visitURI(nodeUri: Node_URI, s: String): NodeInfo = {
    val shortForm = prefixMapping.shortForm(s)
    val (prefix, qname) = {
      val colonIdx = shortForm.indexOf(':')
      if (colonIdx > 0) {
        (Some(shortForm.slice(0, colonIdx)), Some(shortForm.slice(colonIdx + 1, shortForm.length)))
      } else {
        val slashIdx = shortForm.lastIndexOf('/')
        if (slashIdx > 0) {
          (nullRes, Some(shortForm.slice(slashIdx + 1, shortForm.length)))
        } else {
          (nullRes, nullRes)
        }
      }
    }
    if (qname.forall(_.matches("^[QP]\\d+$"))){
      incWdNode(qname.get)
    }
    prefix.foreach(p => incPrefix(p))
    incNode(s"NODE_URI[$shortForm]")
    NodeInfo("NODE_URI", shortForm)
  }

  override def visitVariable(nodeVariable: Node_Variable, s: String): NodeInfo = {
    incNode(s"NODE_VAR[$s]")
    NodeInfo("NODE_VAR", s)
  }
}
