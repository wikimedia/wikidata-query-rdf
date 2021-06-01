package org.wikidata.query.rdf.spark


import scala.util.matching.Regex

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BooleanType, DateType, HiveStringType, NumericType, StringType, TimestampType}

object SparkUtils {
  private val partitionRegex: Regex = "^([\\w]+)=([\\w.-/]+)$".r

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
          case t @ (_: StringType | _: NumericType | _: HiveStringType | _: BooleanType | _: DateType | _: TimestampType) => df(e.name).cast(t)
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

  private def applyTablePartitions[E,O](
                                       tableAndPartitionSpecs: String,
                                       tablePreOp: String => E,
                                       partitionOp: (String, String, E) => E,
                                       tablePostOp: (String, E) => O
                                     )(implicit spark: SparkSession): O = {
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
}
