package org.wikidata.query.rdf.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.flatspec.AnyFlatSpec

trait SparkDataFrameComparisons extends AnyFlatSpec with SparkSessionProvider {

  // explode on a MapType will create two columns called 'key' and 'value' and each element of the
  // map will be listed as a row. This will increase the number of rows greatly if the map contains
  // a lot of elements.
  def explodeAllMapColumns(df: DataFrame): DataFrame = {
    val mapColumns = df.schema.fields.withFilter(x => x.dataType.typeName == "map").map(x => x.name)
    var retDf = df
    for (column <- mapColumns) {
      retDf = retDf.select(col("*"), explode(col(column)))
        .withColumnRenamed("key", column + "_key")
        .withColumnRenamed("value", column + "_value")
        .drop(column)
    }
    retDf
  }

  def assertDataFrameDataEqualsImproved(expected: DataFrame, result: DataFrame): Unit = {
    assertDataFrameDataEquals(explodeAllMapColumns(expected), explodeAllMapColumns(result))
  }
}
