package org.wikidata.query.rdf.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.holdenkarau.spark.testing.SparkSessionProvider.sparkSession
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}

import java.io.File
import java.nio.file.Files

trait SparkSessionProvider extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with DataFrameSuiteBase {

  // same thing in these variables:
  // spark (from DataFrameSuiteBaseLike), sparkSession (from DataFrameSuiteBase), sparkImplicit
  protected implicit var sparkImplicit: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession.conf.set("spark.local.dir", SparkSessionProvider.sparkDir.getAbsolutePath)
    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sparkImplicit = sparkSession
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
}
