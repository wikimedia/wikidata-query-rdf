package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.spark.SparkSessionProvider

class NTripleGeneratorTest extends AnyFlatSpec  with SparkSessionProvider with Matchers {
  "a conforming source table" should "generate output" in {
    val sourceTableDir = newSparkSubDir("test_n_triple_generator")
    val destDir = newSparkSubDir("dest")
    val sourceTableName = "rdf"
    NTripleGeneratorTest.createSimpleQuadTable(sourceTableName, sourceTableDir, spark)
    NTripleGenerator.generateNTriples(sourceTableName, destDir, "my-dump-%02d.nt.gz", 2)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path(destDir)) map { _.getPath.getName } should contain only(
      "my-dump-01.nt.gz", "my-dump-02.nt.gz", "_SUCCESS", "_RENAMED")
  }

  "a non-conforming source table" should "result in an exception" in {
    val sourceTableDirJunk = newSparkSubDir("test_n_triple_generator_junk")
    val destDirJunk = newSparkSubDir("dest_junk")
    val sourceTableNameJunk = "rdf_junk"
    NTripleGeneratorTest.createJunkTable(sourceTableNameJunk, sourceTableDirJunk, spark)
    intercept[AnalysisException] {
      NTripleGenerator.generateNTriples(sourceTableNameJunk, destDirJunk, "dump-%d.gz", 2)
    }
  }

  object NTripleGeneratorTest {
    def createSimpleQuadTable(tableName: String, dir: String, spark: SparkSession): Unit = {
      spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (" +
        "context STRING," +
        "subject STRING," +
        "predicate STRING," +
        "object STRING" +
        s""") USING parquet LOCATION \"$dir\""""
      )
      for (i <- 1 to 100) {
        spark.sql(s"INSERT INTO $tableName VALUES (" +
          s"'<http://localhost/Q$i>', " +
          s"'<http://localhost/Q$i>', " +
          s"'<http://localhost/P$i>', " +
          s"'<http://localhost/O$i>')"
        )
      }
    }
    def createJunkTable(tableName: String, dir: String, spark: SparkSession): Unit = {
      spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName (" +
        "junk STRING," +
        "subject STRING," +
        "predicate STRING," +
        "object STRING" +
        s""") USING parquet LOCATION \"$dir\""""
      )
    }
  }
}
