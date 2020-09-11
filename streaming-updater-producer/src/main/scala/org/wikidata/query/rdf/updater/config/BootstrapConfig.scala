package org.wikidata.query.rdf.updater.config

import org.apache.flink.api.java.utils.ParameterTool

case class BootstrapConfig(args: Array[String]) extends BaseConfig()(ParameterTool.fromArgs(args)) {
  val revisionsFile: String = getStringParam("revisions_file")
  val savepointDir: String = getStringParam("savepoint_dir")
  val parallelism: Int =  getIntParam("parallelism")
}
