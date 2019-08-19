#!/bin/bash
DEBUG=
if [ "$1x" = "-dx" -o "$1x" = "--debugx" ]; then
        shift
		DEBUG=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8001
fi
java -cp target/wikidata-query-tools-*-SNAPSHOT-jar-with-dependencies.jar $DEBUG \
    -DRDFRepositoryMaxPostSize=20000000 \
    org.wikidata.query.rdf.tool.Update --sparqlUrl http://localhost:9999/bigdata/namespace/wdq/sparql $*

