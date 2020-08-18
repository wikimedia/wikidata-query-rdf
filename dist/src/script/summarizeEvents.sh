#!/usr/bin/env bash

CP=`dirname $BASH_SOURCE`/lib/wikidata-query-tools-*-jar-with-dependencies.jar
MAIN=org.wikidata.query.rdf.tool.eventsummmary.Summarizer
java -cp $CP $MAIN "$@"