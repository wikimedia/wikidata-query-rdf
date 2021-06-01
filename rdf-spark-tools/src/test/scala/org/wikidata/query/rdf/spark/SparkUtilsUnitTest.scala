package org.wikidata.query.rdf.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.Matchers

class SparkUtilsUnitTest extends SparkSessionProvider with Matchers {
  val dbName = "SparkUtils"
  val tableName = "UnitTest_test"
  val tableNameNoPart = "UnitTest_nopart"

  "when I want to read a table a table" should "be specifiable" in {
    SparkUtils.readTablePartition(s"$dbName.$tableName").count() shouldBe 2
  }

  "when I want to read a table and a partition both" should "be specifiable" in {
    val rows = SparkUtils.readTablePartition(s"$dbName.$tableName/year=2020/day=01/hour=0/month=07")
      .select("data").take(2)
    rows.length shouldBe 1
    rows(0).getString(0) shouldBe "jul first at noon"
  }

  "when I want to write into table a table and a partition" should "be specifiable" in {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("jul second at noon event 1")),
      Row.fromSeq(Seq("jul second at noon event 2"))
    )), StructType(Seq(StructField("data", StringType, nullable = false))))
    SparkUtils.insertIntoTablePartition(s"$dbName.$tableName/year=2020/day=02/month=07/hour=0000", df)

    val dfT = spark.read.table(s"$dbName.$tableName")

    val rowsDf = dfT
      .filter(dfT("year").equalTo(lit("2020")))
      .filter(dfT("month").equalTo(lit("07")))
      .filter(dfT("day").equalTo(lit(2)))
      .filter(dfT("hour").equalTo(lit(0)))
      .select("data")

    val rows = rowsDf.take(10)

    rows.length shouldBe 2
    rows.map(_.getString(0)) should contain theSameElementsAs Seq("jul second at noon event 1", "jul second at noon event 2")

    // test overwrite
    val dfRewritten = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("jul second at noon event 3")),
      Row.fromSeq(Seq("jul second at noon event 4"))
    )), StructType(Seq(StructField("data", StringType, nullable = false))))
    SparkUtils.insertIntoTablePartition(s"$dbName.$tableName/year=2020/day=2/month=07/hour=0", dfRewritten)

    val rowsRewritten = rowsDf.take(10)

    rowsRewritten.length shouldBe 2
    rowsRewritten.map(_.getString(0)) should contain theSameElementsAs Seq("jul second at noon event 3", "jul second at noon event 4")
  }

  "when I want to write into a table a table" should "be specifiable" in {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("data 1")),
      Row.fromSeq(Seq("data 2"))
    )), StructType(Seq(StructField("data", StringType, nullable = false))))
    SparkUtils.insertIntoTablePartition(s"$dbName.$tableNameNoPart", df)

    val dfT = spark.read.table(s"$dbName.$tableNameNoPart");
    val rows = dfT.select("data")
      .take(10)

    rows.length shouldBe 2
    rows.map(_.getString(0)) should contain theSameElementsAs Seq("data 1", "data 2")
  }

  override def beforeAll() {
    super.beforeAll()
    val db_dir = newSparkSubDir(dbName)
    val table_dir = newSparkSubDir(dbName + "-" + tableName)
    val table_dir_nopart = newSparkSubDir(dbName + "-" + tableNameNoPart)
    spark.sql(s"""CREATE DATABASE $dbName LOCATION \"$db_dir\"""")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $dbName.$tableName (" +
      "data STRING," +
      "year STRING," +
      "month STRING," +
      "day INTEGER," +
      "hour INTEGER" +
      s""") USING parquet PARTITIONED BY (year, month, day, hour) LOCATION \"$table_dir\""""
    )
    spark.sql(s"CREATE TABLE IF NOT EXISTS $dbName.$tableNameNoPart (" +
      "data STRING" +
      s""") USING parquet LOCATION \"$table_dir_nopart\""""
    )
    val schema = StructType(Seq(
      StructField("data", StringType, nullable = false),
      StructField("year", StringType, nullable = false),
      StructField("month", StringType, nullable = false),
      StructField("day", IntegerType, nullable = false),
      StructField("hour", IntegerType, nullable = false)
    ))

    spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row.fromTuple(("jul first at noon", "2020", "07", 1, 0)),
      Row.fromTuple(("Jul first at 1am", "2020", "07", 1, 1))
    )), schema).write.insertInto(s"$dbName.$tableName")
  }
}
