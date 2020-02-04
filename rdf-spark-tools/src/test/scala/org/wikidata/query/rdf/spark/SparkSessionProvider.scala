package org.wikidata.query.rdf.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }
}
