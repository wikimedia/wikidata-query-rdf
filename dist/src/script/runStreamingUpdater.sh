#!/usr/bin/env bash
set -e

QUERY_SERVICE=${QUERY_SERVICE:-"wdqs"}
if [ -r /etc/default/query-service-updater ]; then
  . /etc/default/query-service-updater
fi

HOST=http://localhost:9999
CONTEXT=bigdata
HEAP_SIZE=${HEAP_SIZE:-"1g"}
MEMORY=${MEMORY:-"-Xmx${HEAP_SIZE}"}
LOG_DIR=${LOG_DIR:-"/var/log/query_service"}
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [[ "$JAVA_MAJOR_VERSION" -ge "9" ]] ; then
    GC_LOGS=${GC_LOGS:-"-Xlog:gc:${LOG_DIR}/${QUERY_SERVICE}-streaming-updater_jvm_gc.%p.log \
         -Xlog:gc* \
         -XX:+UnlockExperimentalVMOptions \
         -XX:G1NewSizePercent=20 \
         -XX:+ParallelRefProcEnabled"}
else
    GC_LOGS=${GC_LOGS:-"-Xloggc:${LOG_DIR}/${QUERY_SERVICE}-streaming-updater_jvm_gc.%p.log \
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
fi
EXTRA_JVM_OPTS=${EXTRA_JVM_OPTS:-""}
LOG_CONFIG=${LOG_CONFIG:-""}
NAMESPACE=wdq
UPDATER_OPTS=${UPDATER_OPTS:-""}

while getopts h:c:n:t option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    n) NAMESPACE=${OPTARG};;
    t) TMO=${OPTARG};;
  esac
done

# allow extra args
shift $((OPTIND-1))

if [ -z "$NAMESPACE" ]
then
  echo "Usage: $0 -n <namespace> [-h <host>] [-c <context>] [-S] [-v]"
  exit 1
fi

if [ -z "$TMO" ]; then
    TIMEOUT_ARG=
else
    TIMEOUT_ARG="-Dorg.wikidata.query.rdf.tool.rdf.RdfRepositoryUpdater.timeout=$TMO"
fi

LOG_OPTIONS=""
if [ ! -z "$LOG_CONFIG" ]; then
    LOG_OPTIONS="-Dlogback.configurationFile=${LOG_CONFIG}"
fi

CP=`dirname $BASH_SOURCE`/lib/streaming-updater-consumer-*-jar-with-dependencies.jar
MAIN=org.wikidata.query.rdf.updater.consumer.StreamingUpdate
SPARQL_URL=$HOST/$CONTEXT/namespace/$NAMESPACE/sparql
echo "Updating via $SPARQL_URL"
exec java -cp ${CP} ${MEMORY} ${GC_LOGS} ${LOG_OPTIONS} ${EXTRA_JVM_OPTS} \
     ${TIMEOUT_ARG} ${UPDATER_OPTS} \
     ${MAIN} --sparqlUrl ${SPARQL_URL} "$@"
