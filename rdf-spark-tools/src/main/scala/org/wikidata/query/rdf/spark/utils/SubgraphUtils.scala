package org.wikidata.query.rdf.spark.utils

import org.apache.jena.shared.impl.PrefixMappingImpl
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions._
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.rio.ntriples.NTriplesUtil

object SubgraphUtils {

  /**
   * Abbreviates URI to its prefixes. Uses Apache Jena's PrefixMappingImpl.shortForm.
   *
   * Note: Jena has several `shortForm` implementations, see PrefixMappingBase,
   * PrefixMappingMem, and PrefixMappingImpl. All of them check for substring
   * match from the beginning of the uri and return any one of the matched prefixes.
   *
   * Example problematic cases:
   * http://www.wikidata.org/prop/statement/P21 --> correctly prefixed as ps:P21
   * http://www.wikidata.org/prop/statement/value/P569 --> prefixed as ps:value/P569
   * but should be psv:P569
   *
   * Therefore instead of returning any matched prefix, the method should ideally look
   * for the longest match. That is, between matches A/B/ and A/B/C/, A/B/C/ should be
   * chosen.
   *
   * Another way would be to traverse the URI from the end and keep trying to match until
   * a prefix is found. For /prop/statement/ and /prop/statement/value/, /ontology# and
   * /ontology#API/ etc, the longest match will be returned. This is implemented below
   * in [[uriToPrefixOwnImpl]].
   *
   * Although we have an implementation, we use Jena's shortForm method. This ensures the
   * prefix, right or wrong, is consistent with what we get from [[QueryInfo]]. This is
   * important because the purpose of [[uriToPrefix]] ultimately here is to match prefixed
   * names from Wikidata to prefixed names in SPARQL queries and perform calculation with
   * them.
   */
  val uriToPrefix = udf((rdfUri: String) => {
    val prefixMap: Map[String, String] = getUriToPrefixMapping()
    val prefixMappingImpl = new PrefixMappingImpl()
    prefixMap.foreach { case (uri, prefix) => prefixMappingImpl.setNsPrefix(prefix, uri) }
    prefixMappingImpl.shortForm(removeAngleBrackets(rdfUri))
  })

  /**
   * Abbreviates URI to its prefix form. Example cases to handle:
   * - http://schema.org/                         (schema:)
   * - http://www.w3.org/ns/prov#                 (prov:)
   * - https://www.mediawiki.org/ontology#        (mediawiki:)
   * - https://www.mediawiki.org/ontology#API/    (mwapi:)
   * - https://www.mediawiki.org/ontology#XYZ/ABC (mediawiki:XYZ/ABC)
   */
  val uriToPrefixOwnImpl = udf((rdfUri: String) => {

    val prefixMap: Map[String, String] = getUriToPrefixMapping()
    val uri: String = removeAngleBrackets(rdfUri)
    var shortUri: String = uri
    var ind: Int = uri.length
    var notFound: Boolean = true

    while (ind > 0 && notFound) {

      val currentSubstr = uri.substring(0, ind)

      if (prefixMap.contains(currentSubstr)) {
        val uriPrefix = prefixMap(currentSubstr)
        val uriSuffix = uri.substring(ind, uri.length)
        shortUri = s"$uriPrefix:$uriSuffix"
        notFound = false
      }

      ind -= 1
    }

    shortUri
  })

  // Can be used to extract Q/P-id from URI or node value from NODE_x[] or PATH[]
  // by sending appropriate prefixes
  val extractItem = udf(
    (uri: String, prefix: String) => {
      var value = uri.split(prefix).last.dropRight(1)
      if (prefix != "/") {
        value = value.substring(1) // to remove the [ in NODE_x[] or PATH[]
      }
      value
    }
  )

  // Removes quotes ("") from literals. E.g "lit"@ext or "lit"^^<dtype> -> lit or lit@ext or lit^^<dtype>
  val cleanLiteral = udf((uri: String, removeExtension: Boolean) => {

    val valueFactory = new ValueFactoryImpl()
    val parsedUri = NTriplesUtil.parseLiteral(uri, valueFactory)
    val literal = parsedUri.getLabel
    val language = parsedUri.getLanguage
    val dataType = parsedUri.getDatatype.toString

    if (removeExtension) {
      literal
    }
    else if (language != null) {
      literal + "@" + language
    }
    else if (dataType != null) {
      literal + "^^" + dataType
    }
    else {
      literal
    }
  }
  )

  def getUriToPrefixMapping(): Map[String, String] = {

    // list of prefixes in the format "PREFIX $prefix: <$namespace>"
    val prefixes = PrefixDeclarations.getPrefixDeclarations.split('\n')

    // Removes "PREFIX " portion with substring
    // and turns into map of uri -> abbr
    prefixes.map(_.substring(7).split(":", 2))
      .map(elem => elem.last.substring(2).dropRight(1) -> elem.head)
      .toMap
  }

  // Removes < and > from URIs
  def removeAngleBrackets(uri: String): String = {
    if (uri.charAt(0) == '<' && uri.last == '>') {
      uri.substring(1, uri.length - 1)
    }
    else {
      uri
    }
  }

  // Returns a string of the format "percentile(col, array(0.1,0.2...0.9) as newCol)"
  // to get percent distribution of any columns
  def getPercentileExpr(colName: String, newColName: String): String = {
    val percentileList: String = (1 to 9).map(p => p / 10).map(p => f"$p%.1f").mkString(",")
    f"percentile($colName, array($percentileList)) as $newColName"
  }

  // Converts two columns into a map.
  // Example: colA: a,b,c; colB: 1,2,3 ==> {a->1, b->2, c->3}
  def sparkDfColumnsToMap(df: DataFrame,
                          keyColumn: String,
                          valueColumn: String,
                          mappedColumn: String,
                          groupByColumns: List[String]): DataFrame = {

    val dfCols = groupByColumns.map(name => col(name)) :+
      map_from_arrays(col(keyColumn), col(valueColumn)).as(mappedColumn)

    // scalastyle:off null
    var groupedDf: RelationalGroupedDataset = null
    // scalastyle:on null

    if (groupByColumns.isEmpty) {
      groupedDf = df.groupBy()
    }
    else {
      groupedDf = df.groupBy(groupByColumns.head, groupByColumns.tail: _*)
    }

    groupedDf
      .agg(
        collect_list(keyColumn).as(keyColumn),
        collect_list(valueColumn).as(valueColumn)
      )
      .select(dfCols: _*)
  }

  // Converts a list of columns into a single list of structs.
  // Example: colA: a,b,c; colB: 1,2,3 ==>
  //          [struct (colA: a, colB:1), struct (colA: b, colB:2), struct (colA: c, colB:3)]
  def sparkDfColumnsToListOfStruct(df: DataFrame,
                                   columnsInStruct: List[String],
                                   finalColumn: String,
                                   groupByColumns: List[String]): DataFrame = {

    val dfCols = groupByColumns.map(name => col(name)) :+
      struct(columnsInStruct.head, columnsInStruct.tail: _*).as(finalColumn)

    // scalastyle:off null
    var groupedDf: RelationalGroupedDataset = null
    // scalastyle:on null
    val dfSelected = df.select(dfCols: _*)

    if (groupByColumns.isEmpty) {
      groupedDf = dfSelected.groupBy()
    }
    else {
      groupedDf = dfSelected.groupBy(groupByColumns.head, groupByColumns.tail: _*)
    }

    groupedDf.agg(collect_list(finalColumn).as(finalColumn))
  }

}
