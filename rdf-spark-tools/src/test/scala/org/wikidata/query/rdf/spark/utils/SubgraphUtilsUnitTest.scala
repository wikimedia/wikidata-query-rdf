package org.wikidata.query.rdf.spark.utils

import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.Matchers
import org.wikidata.query.rdf.spark.SparkDataFrameComparisons
import org.wikidata.query.rdf.spark.utils.SubgraphUtils._

case class personInfo(name: String, country: String, age: Int)

class SubgraphUtilsUnitTest extends SparkDataFrameComparisons with Matchers {

  "Rdf URIs" should "be converted to prefixes" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      "http://schema.org/about",
      "http://www.w3.org/2000/01/rdf-schema#label",
      "https://www.mediawiki.org/ontology#XYZ",
      "https://www.mediawiki.org/ontology#API/item",
      "https://www.mediawiki.org/ontology#XYZ/ABC",
      "http://www.wikidata.org/entity/Q42",
      "http://www.wikidata.org/entity/statement/Q42-7eb8aaef-4ddf-8b87-bd02-406f91a296bd",
      "http://www.wikidata.org/prop/P106",
      "http://www.wikidata.org/prop/direct/P31",
      "http://www.wikidata.org/prop/direct-normalized/P1015",
      "http://www.wikidata.org/prop/statement/P21",
      "<http://www.wikidata.org/prop/statement/value/P569>",
      "<http://www.wikidata.org/prop/qualifier/P625>",
      "<http://www.wikidata.org/prop/reference/P248>",
      "<http://www.wikidata.org/prop/novalue/P22>"
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("uri")
    val result = testDF.select(uriToPrefix(col("uri"))).as[String].collect
    result shouldBe Array(
      "schema:about",
      "rdfs:label",
      "mediawiki:XYZ",
      "mwapi:item",
      "mediawiki:XYZ/ABC",
      "wd:Q42",
      "wds:Q42-7eb8aaef-4ddf-8b87-bd02-406f91a296bd",
      "p:P106",
      "wdt:P31",
      "wdtn:P1015",
      "ps:P21",
      "ps:value/P569", // should be psv:P569, but Jena parses it as ps:value/P569
      "pq:P625",
      "pr:P248",
      "p:novalue/P22" // should be wdno:P22, but Jena parses it as p:novalue/P22
    )
  }

  it should "be converted to prefixes using own implementation" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      "http://schema.org/about",
      "http://www.w3.org/2000/01/rdf-schema#label",
      "https://www.mediawiki.org/ontology#XYZ",
      "https://www.mediawiki.org/ontology#API/item",
      "https://www.mediawiki.org/ontology#XYZ/ABC",
      "http://www.wikidata.org/entity/Q42",
      "http://www.wikidata.org/entity/statement/Q42-7eb8aaef-4ddf-8b87-bd02-406f91a296bd",
      "http://www.wikidata.org/prop/P106",
      "http://www.wikidata.org/prop/direct/P31",
      "http://www.wikidata.org/prop/direct-normalized/P1015",
      "http://www.wikidata.org/prop/statement/P21",
      "http://www.wikidata.org/prop/statement/value/P569",
      "http://www.wikidata.org/prop/qualifier/P625",
      "http://www.wikidata.org/prop/reference/P248",
      "http://www.wikidata.org/prop/novalue/P22"
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("uri")
    val result = testDF.select(uriToPrefixOwnImpl(col("uri"))).as[String].collect
    result shouldBe Array(
      "schema:about",
      "rdfs:label",
      "mediawiki:XYZ",
      "mwapi:item",
      "mediawiki:XYZ/ABC",
      "wd:Q42",
      "wds:Q42-7eb8aaef-4ddf-8b87-bd02-406f91a296bd",
      "p:P106",
      "wdt:P31",
      "wdtn:P1015",
      "ps:P21",
      "psv:P569",
      "pq:P625",
      "pr:P248",
      "wdno:P22"
    )
  }

  "item" should "be properly extracted from URI" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      "<http://schema.org/about>",
      "<https://www.mediawiki.org/ontology#XYZ>",
      "<http://www.wikidata.org/entity/Q42>",
      "<http://www.wikidata.org/entity/Q16222597>",
      "<http://www.wikidata.org/prop/P106>",
      "<http://www.wikidata.org/prop/direct/P31>",
      "<http://www.wikidata.org/prop/statement/P21>",
      "<http://www.wikidata.org/prop/qualifier/P625>"
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("uri")
    val result = testDF.select(extractItem(col("uri"), lit("/"))).as[String].collect
    result shouldBe Array(
      "about",
      "ontology#XYZ",
      "Q42",
      "Q16222597",
      "P106",
      "P31",
      "P21",
      "P625"
    )
  }

  "node value" should "be properly extracted from query node" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      "NODE_URI[https://en.wikipedia.org/]",
      "NODE_URI[wdt:P279]",
      "NODE_URI[hint:rangeSafe]",
      "NODE_URI[bd:serviceParam]",
      "NODE_URI[ps:value/P281]"
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("uri")
    val result = testDF.select(extractItem(col("uri"), lit("NODE_URI"))).as[String].collect
    result shouldBe Array(
      "https://en.wikipedia.org/",
      "wdt:P279",
      "hint:rangeSafe",
      "bd:serviceParam",
      "ps:value/P281"
    )
  }

  "literal value" should "be properly extracted from literal nodes" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      "\"scientific article published on 3 July 1932\"@en",
      "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
      "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
      "\"artikull shkencor\"@sq",
      "\"Linda McCall\""
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("literal")
    val resultWithoutExtension = testDF.select(cleanLiteral(col("literal"), lit(true))).as[String].collect
    resultWithoutExtension shouldBe Array(
      "scientific article published on 3 July 1932",
      "true",
      "1",
      "artikull shkencor",
      "Linda McCall"
    )

    val resultWithExtension = testDF.select(cleanLiteral(col("literal"), lit(false))).as[String].collect
    resultWithExtension shouldBe Array(
      "scientific article published on 3 July 1932@en",
      "true^^http://www.w3.org/2001/XMLSchema#boolean",
      "1^^http://www.w3.org/2001/XMLSchema#integer",
      "artikull shkencor@sq",
      "Linda McCall^^http://www.w3.org/2001/XMLSchema#string"
    )
  }

  "Two spark df columns to map conversion" should "be possible for multiple groups" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      ("group1", "A", 1), ("group1", "B", 2), ("group1", "C", 3), ("group1", "D", 4),
      ("group2", "P", 16), ("group2", "Q", 17), ("group2", "R", 18), ("group2", "S", 19),
      ("group3", "W", 23), ("group3", "X", 24), ("group3", "Y", 25), ("group3", "Z", 26)
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("group", "key_column", "value_column")
    val resultDF = sparkDfColumnsToMap(testDF, "key_column", "value_column", "mapped_column", List("group"))

    val expectedResultData = Seq(
      ("group1", Map("D" -> 4, "A" -> 1, "B" -> 2, "C" -> 3)),
      ("group3", Map("Y" -> 25, "W" -> 23, "X" -> 24, "Z" -> 26)),
      ("group2", Map("P" -> 16, "Q" -> 17, "R" -> 18, "S" -> 19))
    )
    val expectedResultDF = spark.sparkContext.parallelize(expectedResultData).toDF("group", "mapped_column")

    assertDataFrameDataEqualsImproved(resultDF, expectedResultDF)
  }

  it should "be possible when grouping the entire df" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(("A", 1), ("B", 2), ("C", 3), ("D", 4))

    val testDF = spark.sparkContext.parallelize(data).toDF("key_column", "value_column")
    val resultDF = sparkDfColumnsToMap(testDF, "key_column", "value_column", "mapped_column", List())

    val expectedResultData = Seq((Map("A" -> 1, "B" -> 2, "D" -> 4, "C" -> 3)))
    val expectedResultDF = spark.sparkContext.parallelize(expectedResultData).toDF("mapped_column")

    assertDataFrameDataEqualsImproved(resultDF, expectedResultDF)
  }

  "Two or more spark dataframe columns to struct conversion" should "be possible for multiple groups" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      ("group1", "andy", "US", 109), ("group1", "beth", "Canada", 10), ("group1", "cary", "Ireland", 59),
      ("group2", "emily", "Greenland", 56), ("group2", "frans", "Iceland", 19),
      ("group3", "ivany", "Mexico", 90)
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("group", "name", "country", "age")
    val resultDF = sparkDfColumnsToListOfStruct(testDF, List("name", "country", "age"), "person_info", List("group"))

    val expectedResultData = Seq(
      ("group1", Array(personInfo("cary", "Ireland", 59), personInfo("andy", "US", 109), personInfo("beth", "Canada", 10))),
      ("group3", Array(personInfo("ivany", "Mexico", 90))),
      ("group2", Array(personInfo("frans", "Iceland", 19), personInfo("emily", "Greenland", 56)))
    )
    val expectedResultDF = spark.sparkContext.parallelize(expectedResultData).toDF("group", "person_info")

    assertDataFrameDataEqualsImproved(resultDF, expectedResultDF)
  }

  it should "be possible when grouping the entire df" in {
    // scalastyle:off import.grouping
    import spark.implicits._
    // scalastyle:on import.grouping

    val data = Seq(
      ("andy", "US", 109), ("beth", "Canada", 10), ("cary", "Ireland", 59)
    )

    val testDF = spark.sparkContext.parallelize(data).toDF("name", "country", "age")
    val resultDF = sparkDfColumnsToListOfStruct(testDF, List("name", "country", "age"), "person_info", List())

    val expectedResultData = Seq((Array(personInfo("beth", "Canada", 10), personInfo("andy", "US", 109), personInfo("cary", "Ireland", 59))))
    val expectedResultDF = spark.sparkContext.parallelize(expectedResultData).toDF("person_info")

    assertDataFrameDataEqualsImproved(resultDF, expectedResultDF)
  }

}
