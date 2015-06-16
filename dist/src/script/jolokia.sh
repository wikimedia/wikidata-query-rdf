#!/usr/bin/env bash
# Use: jolokia.sh {start|stop}

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
JAR=$DIR/lib/jolokia-jvm-*-agent.jar
MAIN=org.wikidata.query.rdf.tool.Update
java -jar $JAR $1 $MAIN 
