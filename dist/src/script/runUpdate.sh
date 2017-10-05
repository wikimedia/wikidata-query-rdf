#!/usr/bin/env bash

if [ -r /etc/default/wdqs-updater ]; then
  . /etc/default/wdqs-updater
fi

HOST=http://localhost:9999
CONTEXT=bigdata
MEMORY="-Xms2g -Xmx2g"
GC_LOGS="-Xloggc:/var/log/wdqs/wdqs-updater_jvm_gc.%p.log \
         -XX:+PrintGCDetails \
         -XX:+PrintGCDateStamps \
         -XX:+PrintGCTimeStamps \
         -XX:+PrintTenuringDistribution \
         -XX:+PrintGCCause \
         -XX:+PrintGCApplicationStoppedTime \
         -XX:+UseGCLogFileRotation \
         -XX:NumberOfGCLogFiles=10 \
         -XX:GCLogFileSize=20M"
EXTRA_JVM_OPTS=${EXTRA_JVM_OPTS:-""}
LOG_CONFIG=${LOG_CONFIG:-""}
NAMESPACE=wdq
UPDATER_OPTS=${UPDATER_OPTS:-""}

while getopts h:c:n:l:t:s option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    n) NAMESPACE=${OPTARG};;
    l) LANGS=${OPTARG};;
    t) TMO=${OPTARG};;
    s) SKIPSITE=1;;
  esac
done

# allow extra args
shift $((OPTIND-1))

if [ -z "$NAMESPACE" ]
then
  echo "Usage: $0 -n <namespace> [-h <host>] [-c <context>]"
  exit 1
fi

if [ -z "$TMO" ]; then
    TIMEOUT_ARG=
else
    TIMEOUT_ARG="-Dorg.wikidata.query.rdf.tool.rdf.RdfRepository.timeout=$TMO"
fi

if [ -z "$LANGS" ]; then
    ARGS=
else
    ARGS="--labelLanguage $LANGS --singleLabel $LANGS"
fi

if [ ! -z "$SKIPSITE" ]; then
    ARGS="$ARGS --skipSiteLinks"
fi
LOG_OPTIONS=""
if [ ! -z "$LOG_CONFIG" ]; then
    LOG_OPTIONS="-Dlogback.configurationFile=${LOG_CONFIG}"
fi

CP=lib/wikidata-query-tools-*-jar-with-dependencies.jar
MAIN=org.wikidata.query.rdf.tool.Update
SPARQL_URL=$HOST/$CONTEXT/namespace/$NAMESPACE/sparql
AGENT=-javaagent:lib/jolokia-jvm-1.3.1-agent.jar=port=8778,host=localhost
echo "Updating via $SPARQL_URL"
exec java -cp ${CP} ${MEMORY} ${GC_LOGS} ${LOG_OPTIONS} ${EXTRA_JVM_OPTS} \
     ${TIMEOUT_ARG} ${AGENT} ${UPDATER_OPTS} \
     ${MAIN} ${ARGS} --sparqlUrl ${SPARQL_URL} "$@"
