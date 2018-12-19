#!/bin/bash

java -cp target/wikidata-query-tools-*-SNAPSHOT-jar-with-dependencies.jar \
    -DRDFRepositoryMaxPostSize=20000000 \
    org.wikidata.query.rdf.tool.Update --sparqlUrl http://localhost:9999/bigdata/namespace/wdq/sparql $*

