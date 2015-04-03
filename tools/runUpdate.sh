#!/bin/bash

java -cp target/wikidata-query-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.wikidata.query.rdf.tool.Update --sparqlUrl http://localhost:9999/bigdata/namespace/kb/sparql

