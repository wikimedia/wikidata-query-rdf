#!/bin/bash

java -cp target/wikidata-query-tools-*-SNAPSHOT-jar-with-dependencies.jar org.wikidata.query.rdf.tool.Update --sparqlUrl http://localhost:9999/bigdata/namespace/wdq/sparql $*

