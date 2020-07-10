package org.wikidata.query.rdf.spark

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}

trait SparkSessionProvider extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  protected implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSessionProvider.spark
  }

  override def beforeEach(): Unit = {
    FileUtils.forceDeleteOnExit(SparkSessionProvider.sparkDir)
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
    .config("spark.sql.warehouse.dir", new File(SparkSessionProvider.sparkDir, "spark-warehouse").getAbsolutePath)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
}
