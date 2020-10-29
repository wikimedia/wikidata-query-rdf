package org.wikidata.query.rdf.updater.config

case class BootstrapConfig(args: Array[String]) extends BaseConfig()(BaseConfig.params(args)) {
  val revisionsFile: String = getStringParam("revisions_file")
  val savepointDir: String = getStringParam("savepoint_dir")
  val parallelism: Int =  getIntParam("parallelism")
}
