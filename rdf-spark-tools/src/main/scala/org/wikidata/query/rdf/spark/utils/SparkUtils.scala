package org.wikidata.query.rdf.spark.utils

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.regex.Pattern
import scala.util.matching.Regex

object SparkUtils {
  private val partitionRegex: Regex = "^([\\w]+)=([\\w.-/]+)$".r

  def renameSparkPartitions(source: String, format: String, filter: Pattern, flag: Option[String] = Some("_RENAMED"))(implicit spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!Pattern.compile("%\\d*d").matcher(format).find()) {
      throw new IllegalArgumentException(s"Invalid format $format: it should have a %d place-holder for the partition index.")
    }
    fs.listStatus(new Path(source)).filter { file =>
      file.isFile && filter.matcher(file.getPath.getName).find()
    }.zipWithIndex.foreach { case (file: FileStatus, idx: Int) =>
      fs.rename(file.getPath, new Path(file.getPath.getParent, format.format(idx + 1)))
    }
    flag foreach { f =>
      val out = fs.create(new Path(source, f))
      out.writeUTF("renamed")
      out.close()
    }
  }

  def readTablePartition(tableAndPartitionSpecs: String)(implicit spark: SparkSession): DataFrame = {
    applyTablePartitions[DataFrame, DataFrame](tableAndPartitionSpecs,
      spark.read.table,
      (column, value, df) => df.filter(df(column).equalTo(lit(value))),
      (_, df) => df)
  }

  def insertIntoTablePartition(tableAndPartitionSpecs: String,
                               dataFrame: DataFrame,
                               saveMode: SaveMode = SaveMode.Overwrite,
                               format: Option[String] = None
                              )(implicit spark: SparkSession): Unit = {
    def insertIntoFunction(table: String, df: DataFrame): Unit = {
      // reorder the columns according to the target table because
      // DataFrame.insertInto only care about column position
      val dfw = df.select(spark.read.table(table).schema.fields.map(e => {
        e.dataType match {
          case t@(_: StringType | _: VarcharType | _: CharType | _: NumericType | _: BooleanType | _: DateType | _: TimestampType) => df(e.name).cast(t)
          case _ => df(e.name)
        }
      }): _*)
        .write.mode(saveMode)
      format.foreach(dfw.format)
      dfw.insertInto(table)
    }

    applyTablePartitions[DataFrame, Unit](tableAndPartitionSpecs,
      _ => dataFrame,
      (column, value, df) => df.withColumn(column, lit(value)),
      insertIntoFunction)
  }

  private def applyTablePartitions[E, O](
                                          tableAndPartitionSpecs: String,
                                          tablePreOp: String => E,
                                          partitionOp: (String, String, E) => E,
                                          tablePostOp: (String, E) => O
                                        ): O = {
    val tableAndPartitions: Array[String] = tableAndPartitionSpecs.split("/", 2)
    tableAndPartitions match {
      case Array(table, partition) => tablePostOp(table, applyPartitions(tablePreOp(table), partition, partitionOp))
      case Array(table) => tablePostOp(table, tablePreOp(table))
      case _ => throw new IllegalArgumentException("Invalid table or partition specifications: [" + tableAndPartitionSpecs + "]")
    }
  }

  private def applyPartitions[E](input: E, partitionSpec: String, func: (String, String, E) => E): E = {
    var df = input
    partitionSpec.split("/") foreach {
      partitionRegex.findFirstMatchIn(_) match {
        case Some(m) =>
          df = func(m.group(1), m.group(2), df)
        case None =>
          throw new IllegalArgumentException("Invalid partition specifications: [" + partitionSpec + "]")
      }
    }
    df
  }

  def getSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      // required because spark would fail with:
      // Exception in thread "main" org.apache.spark.SparkException: Dynamic partition strict mode requires
      // at least one static partition column. To turn this off set // hive.exec.dynamic.partition.mode=nonstrict
      .config("hive.exec.dynamic.partition", value = true)
      .config("hive.exec.dynamic.partition.mode", "non-strict")
      // Allows overwriting the target partitions
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .appName(appName)
      .getOrCreate()
  }

  def saveTables(dataframeAndPathList: List[(DataFrame, String)])
                (implicit spark: SparkSession): Unit = {

    for ((df, path) <- dataframeAndPathList) {
      insertIntoTablePartition(path, df, saveMode = Overwrite, format = Some("hive"))
    }
  }
}
