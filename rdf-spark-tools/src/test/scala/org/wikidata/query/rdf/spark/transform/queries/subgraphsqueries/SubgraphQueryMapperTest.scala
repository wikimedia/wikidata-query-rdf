package org.wikidata.query.rdf.spark.transform.queries.subgraphsqueries

import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.spark.SparkDataFrameComparisons

class SubgraphQueryMapperTest extends SparkDataFrameComparisons with Matchers {

  var queries: DataFrame = _
  var wikidataTriples: DataFrame = _
  var topSubgraphTriples: DataFrame = _
  var topSubgraphItems: DataFrame = _
  var subgraphQueryMapperUtils: SubgraphQueryMapperUtils = _
  var subgraphQueryMapping: DataFrame = _
  var selectedPredicates: DataFrame = _
  var selectedURIs: DataFrame = _
  var selectedLiterals: DataFrame = _

  "subgraph query mapper utils" should "identify all predicates that occur 99% times in each subgraph" in {
    val selectedPredicatesTestResult = subgraphQueryMapperUtils.getSelectedPredicates()

    assertDataFrameDataEqualsImproved(selectedPredicatesTestResult, selectedPredicates)
  }

  it should "identify all URIs that occur 99% times in each subgraph" in {
    val wikidataNodeMetrics = subgraphQueryMapperUtils.getWikidataNodeMetrics()
    val subgraphNodeMetrics = subgraphQueryMapperUtils.getSubgraphNodeMetrics()
    val selectedURIsTestResult = subgraphQueryMapperUtils.getSelectedURIs(wikidataNodeMetrics, subgraphNodeMetrics)

    assertDataFrameDataEqualsImproved(selectedURIsTestResult, selectedURIs)
  }

  it should "identify all literals that occur 99% times in each subgraph" in {
    val wikidataNodeMetrics = subgraphQueryMapperUtils.getWikidataNodeMetrics()
    val subgraphNodeMetrics = subgraphQueryMapperUtils.getSubgraphNodeMetrics()
    val selectedLiteralsTestResult = subgraphQueryMapperUtils.getSelectedLiterals(wikidataNodeMetrics, subgraphNodeMetrics)

    assertDataFrameDataEqualsImproved(selectedLiteralsTestResult, selectedLiterals)
  }

  "subgraph query mapper" should "correctly map queries to subgraphs along with reason of match" in {
    val (_, _, _, subgraphQueryMappingTestResult) = SubgraphQueryMapper.getSubgraphQueryMapping(
      wikidataTriples,
      queries,
      topSubgraphTriples,
      topSubgraphItems,
      99)

    assertDataFrameDataEqualsImproved(subgraphQueryMappingTestResult, subgraphQueryMapping)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val subgraphsResourcePath = "/org/wikidata/query/rdf/spark/transform/structureddata/subgraphs/"
    wikidataTriples = spark.read.json(getClass.getResource(subgraphsResourcePath + "wikidata_triples_small.json").toString)
    topSubgraphTriples = spark.read.json(getClass.getResource(subgraphsResourcePath + "top_subgraph_triples.json").toString)
    topSubgraphItems = spark.read.json(getClass.getResource(subgraphsResourcePath + "top_subgraph_items.json").toString)

    val schemaSource = Source.fromURL(getClass.getResource("queries_small_sample_schema.json"))
    val schema = DataType.fromJson(schemaSource.getLines.mkString).asInstanceOf[StructType]
    queries = spark.read.schema(schema).json(getClass.getResource("queries_small_sample.json").toString)
    schemaSource.close()

    subgraphQueryMapping = spark.read.json(getClass.getResource("subgraphs_queries_mapping.json").toString)
    selectedPredicates = spark.read.json(getClass.getResource("selected_predicates.json").toString)
    selectedURIs = spark.read.json(getClass.getResource("selected_uris.json").toString)
    selectedLiterals = spark.read.json(getClass.getResource("selected_literals.json").toString)

    subgraphQueryMapperUtils = new SubgraphQueryMapperUtils(wikidataTriples, queries, topSubgraphTriples, 99)
  }
}
