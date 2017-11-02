#!/usr/bin/env bash

if [ -r /etc/default/wdqs-blazegraph ]; then
  . /etc/default/wdqs-blazegraph
fi

HOST=${HOST:-"localhost"}
CONTEXT=bigdata
PORT=${PORT:-"9999"}
DIR=${DIR:-`dirname $0`}
HEAP_SIZE=${HEAP_SIZE:-"16g"}
LOG_CONFIG=${LOG_CONFIG:-""}
LOG_DIR=${LOG_DIR:-"/var/log/wdqs"}
MEMORY=${MEMORY:-"-Xmx${HEAP_SIZE}"}
GC_LOGS=${GC_LOGS:-"-Xloggc:${LOG_DIR}/wdqs-blazegraph_jvm_gc.%p.log \
         -XX:+PrintGCDetails \
         -XX:+PrintGCDateStamps \
         -XX:+PrintGCTimeStamps \
         -XX:+PrintAdaptiveSizePolicy \
         -XX:+PrintReferenceGC \
         -XX:+PrintGCCause \
         -XX:+PrintGCApplicationStoppedTime \
         -XX:+PrintTenuringDistribution \
         -XX:+UseGCLogFileRotation \
         -XX:NumberOfGCLogFiles=10 \
         -XX:GCLogFileSize=20M"}
EXTRA_JVM_OPTS=${EXTRA_JVM_OPTS:-""}
BLAZEGRAPH_OPTS=${BLAZEGRAPH_OPTS:-""}
CONFIG_FILE=${CONFIG_FILE:-"RWStore.properties"}
DEBUG=-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=n
# Disable this for debugging
DEBUG=

function usage() {
  echo "Usage: $0 [-h <host>] [-d <dir>] [-c <context>] [-p <port>] [-o <blazegraph options>] [-f config.properties]"
  exit 1
}

while getopts h:c:p:d:o:f:? option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    p) PORT=${OPTARG};;
    d) DIR=${OPTARG};;
    o) BLAZEGRAPH_OPTS="${OPTARG}";;
    f) CONFIG_FILE=${OPTARG};;
    ?) usage;;
  esac
done

pushd $DIR

# Q-id of the default globe
DEFAULT_GLOBE=2
# Blazegraph HTTP User Agent for federation
USER_AGENT="Wikidata Query Service; https://query.wikidata.org/";

LOG_OPTIONS=""
if [ ! -z "$LOG_CONFIG" ]; then
    LOG_OPTIONS="-Dlogback.configurationFile=${LOG_CONFIG}"
fi

echo "Running Blazegraph from `pwd` on :$PORT/$CONTEXT"
exec java \
     -server -XX:+UseG1GC ${MEMORY} ${DEBUG} ${GC_LOGS} ${LOG_OPTIONS} ${EXTRA_JVM_OPTS} \
     -Dcom.bigdata.rdf.sail.webapp.ConfigParams.propertyFile=${CONFIG_FILE} \
     -Dorg.eclipse.jetty.server.Request.maxFormContentSize=200000000 \
     -Dcom.bigdata.rdf.sparql.ast.QueryHints.analytic=true \
     -Dcom.bigdata.rdf.sparql.ast.QueryHints.analyticMaxMemoryPerQuery=1073741824 \
     -DASTOptimizerClass=org.wikidata.query.rdf.blazegraph.WikibaseOptimizers \
     -Dorg.wikidata.query.rdf.blazegraph.inline.literal.WKTSerializer.noGlobe=$DEFAULT_GLOBE \
     -Dcom.bigdata.rdf.sail.webapp.client.RemoteRepository.maxRequestURLLength=7168 \
     -Dcom.bigdata.rdf.sail.sparql.PrefixDeclProcessor.additionalDeclsFile=$DIR/prefixes.conf \
     -Dorg.wikidata.query.rdf.blazegraph.mwapi.MWApiServiceFactory.config=$DIR/mwservices.json \
     -Dcom.bigdata.rdf.sail.webapp.client.HttpClientConfigurator=org.wikidata.query.rdf.blazegraph.ProxiedHttpConnectionFactory \
     -Dhttp.userAgent="${USER_AGENT}" \
     ${BLAZEGRAPH_OPTS} \
     -jar jetty-runner*.jar \
     --host $HOST \
     --port $PORT \
     --path /$CONTEXT \
     blazegraph-service-*.war
