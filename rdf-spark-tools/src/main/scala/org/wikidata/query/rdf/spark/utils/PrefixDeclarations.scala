package org.wikidata.query.rdf.spark.utils

import java.lang

import org.wikidata.query.rdf.common.uri._

object PrefixDeclarations {

  def getPrefixDeclarations: String = {

    def makeURI(prefix: String, namespace: String): String = s"PREFIX $prefix: <$namespace>"

    val prefixDeclarations = List(
      makeURI("wikibase", Ontology.NAMESPACE),
      makeURI("skos", SKOS.NAMESPACE),
      makeURI("ontolex", Ontolex.NAMESPACE),
      makeURI("mediawiki", Mediawiki.NAMESPACE),
      makeURI("mwapi", Mediawiki.API),
      makeURI("geof", GeoSparql.FUNCTION_NAMESPACE),
      makeURI("geo", GeoSparql.NAMESPACE),
      makeURI("dct", Dct.NAMESPACE),
      makeURI("viaf", CommonValues.VIAF),
      makeURI("owl", OWL.NAMESPACE),
      makeURI("prov", Provenance.NAMESPACE),
      makeURI("rdf", RDF.NAMESPACE),
      makeURI("rdfs", RDFS.NAMESPACE),
      makeURI("schema", SchemaDotOrg.NAMESPACE),
      makeURI("gas", "http://www.bigdata.com/rdf/gas#"),
      makeURI("xsd", "http://www.w3.org/2001/XMLSchema#"),
      makeURI("cc", "http://creativecommons.org/ns#"),
      makeURI("bd", "http://www.bigdata.com/rdf#"),
      makeURI("hint", "http://www.bigdata.com/queryHints#"),
      makeURI("foaf", "http://xmlns.com/foaf/0.1/"),
      makeURI("fn", "http://www.w3.org/2005/xpath-functions#"),
      makeURI("dc", "http://purl.org/dc/elements/1.1/"),
      UrisSchemeFactory.WIKIDATA.prefixes(new lang.StringBuilder()).toString
    )

    prefixDeclarations.mkString("\n")
  }
}
