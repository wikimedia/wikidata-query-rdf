#!/usr/bin/env bash

HOST=localhost
CONTEXT=bigdata
PORT=9999
DIR=`dirname $0`
MEMORY=-Xmx8g
BLAZEGRAPH_OPTS=""

function usage() {
  echo "Usage: $0 [-h <host>] [-d <dir>] [-c <context>] [-p <port>] [-o <blazegraph options>] "
  exit 1
}

while getopts h:c:p:d:o:? option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    p) PORT=${OPTARG};;
    d) DIR=${OPTARG};;
    o) BLAZEGRAPH_OPTS="${OPTARG}";;
    ?) usage;;
  esac
done

pushd $DIR

# Q-id of the default globe
DEFAULT_GLOBE=2

echo "Running Blazegraph from `pwd` on :$PORT/$CONTEXT"
java -server -XX:+UseG1GC ${MEMORY} -Dcom.bigdata.rdf.sail.webapp.ConfigParams.propertyFile=RWStore.properties \
     -Dorg.eclipse.jetty.server.Request.maxFormContentSize=200000000 \
     -Dcom.bigdata.rdf.sparql.ast.QueryHints.analytic=true \
     -Dcom.bigdata.rdf.sparql.ast.QueryHints.analyticMaxMemoryPerQuery=1073741824 \
     -DASTOptimizerClass=org.wikidata.query.rdf.blazegraph.WikibaseOptimizers \
     -Dorg.wikidata.query.rdf.blazegraph.inline.literal.WKTSerializer.noGlobe=$DEFAULT_GLOBE \
     ${BLAZEGRAPH_OPTS} \
     -jar jetty-runner*.jar \
     --host $HOST \
     --port $PORT \
     --path /$CONTEXT \
     blazegraph-service-*.war
