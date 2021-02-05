package org.wikidata.query.rdf.updater.config

import java.nio.file.{Files, Paths}

import org.apache.flink.api.java.utils.ParameterTool

class BaseConfig(protected implicit val params: ParameterTool) {
  val checkpointDir: String = getStringParam("checkpoint_dir")

  def getStringParam(key: String)(implicit parameterTool: ParameterTool): String = getParam(parameterTool.get, key)

  def getIntParam(key: String)(implicit parameterTool: ParameterTool): Int = getParam(parameterTool.getInt, key)

  def getParam[T](getParamFn: (String) => T, key: String): T =
    Option(getParamFn(key)) match {
      case Some(value) => value;
      case None => throw new IllegalArgumentException("missing param " + key)
    }
}

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


