package org.wikidata.query.rdf.spark

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

trait SparkSessionProvider extends FlatSpec with BeforeAndAfterEach  {
  protected var spark: SparkSession = _

  override def beforeEach(): Unit = {
    FileUtils.forceDeleteOnExit(SparkSessionProvider.sparkDir)
    spark = SparkSessionProvider.spark
  }

  def newSparkSubDir(subdir: String): String = {
    new File(SparkSessionProvider.sparkDir, subdir).getAbsolutePath
  }
}

object SparkSessionProvider {
  private lazy val sparkDir = Files.createTempDirectory("spark_local_dir").toFile
  private lazy val spark = SparkSession.builder()
    .master("local")
    .appName("spark test example")
    .config("spark.local.dir", SparkSessionProvider.sparkDir.getAbsolutePath)
    .getOrCreate()
}
