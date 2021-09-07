package org.wikidata.query.rdf.updater.config

import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.flink.api.java.utils.ParameterTool

class BaseConfig(protected implicit val params: ParameterTool) {
  def getStringParam(key: String)(implicit parameterTool: ParameterTool): String = getParam(parameterTool.get, key)

  def getIntParam(key: String)(implicit parameterTool: ParameterTool): Int = getParam(parameterTool.getInt, key)

  def getParam[T](getParamFn: (String) => T, key: String): T =
    Option(getParamFn(key)) match {
      case Some(value) => value;
      case None => throw new IllegalArgumentException("missing param " + key)
    }

  def getInputKafkaTopics: InputKafkaTopics = {
    InputKafkaTopics(
      revisionCreateTopicName = getStringParam("rev_create_topic"),
      pageDeleteTopicName = getStringParam("page_delete_topic"),
      pageUndeleteTopicName = getStringParam("page_undelete_topic"),
      suppressedDeleteTopicName = getStringParam("suppressed_delete_topic"),
      reconciliationTopicName = optionalFilteredReconciliationTopic("reconciliation_topic"),
      topicPrefixes = params.get("topic_prefixes", "").split(",").toList
    )
  }

  def optionalIntArg(paramName: String)(implicit params: ParameterTool): Option[Int] = {
    if (params.has(paramName)) {
      Some(params.getInt(paramName))
    } else {
      None
    }
  }

  def optionalStringArg(paramName: String)(implicit params: ParameterTool): Option[String] = {
    if (params.has(paramName)) {
      Some(params.get(paramName))
    } else {
      None
    }
  }

  def optionalUriArg(paramName: String)(implicit params: ParameterTool): Option[URI] = {
    optionalStringArg(paramName).map(URI.create)
  }

  /**
   * match things like:
   * - topic.name[source_filter_eqiad] => FilteredReconciliationTopic("topic.name", Some("source_filter_eqiad"))
   * - topic.name => FilteredReconciliationTopic("topic.name", None)
   */
  private val filteredTopicPattern = "^([^\\[\\]]+)(\\[([^]]+)])?$".r
  def optionalFilteredReconciliationTopic(paramName: String)(implicit params: ParameterTool): Option[FilteredReconciliationTopic] = {
    optionalStringArg(paramName) map filteredTopicPattern.findFirstMatchIn map {
      case Some(m) => FilteredReconciliationTopic(m.group(1), Option(m.group(3)))
      case None => throw new IllegalArgumentException(s"Cannot parse [${getStringParam(paramName)}] as a filtered topic")
    }
  }
}

sealed case class InputKafkaTopics(revisionCreateTopicName: String,
                                   pageDeleteTopicName: String,
                                   pageUndeleteTopicName: String,
                                   suppressedDeleteTopicName: String,
                                   reconciliationTopicName: Option[FilteredReconciliationTopic],
                                   topicPrefixes: List[String])

case class FilteredReconciliationTopic(topic: String, source: Option[String])

object BaseConfig {
  val MAX_PARALLELISM: Int = 1024
  def params(argv: Array[String]): ParameterTool = {
    argv match {
      case Array(filePath) if Files.isReadable(Paths.get(filePath)) =>
        ParameterTool.fromPropertiesFile(filePath)
      case _ =>
        ParameterTool.fromArgs(argv)
    }
  }
}
