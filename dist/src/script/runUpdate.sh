#!/usr/bin/env bash
set -e

if [ -r /etc/default/wdqs-updater ]; then
  . /etc/default/wdqs-updater
fi

HOST=http://localhost:9999
CONTEXT=bigdata
HEAP_SIZE=${HEAP_SIZE:-"3g"}
MEMORY=${MEMORY:-"-Xmx${HEAP_SIZE}"}
LOG_DIR=${LOG_DIR:-"/var/log/wdqs"}
if [[ "$JAVA_MAJOR_VERSION" -ge "9" ]] ; then
    GC_LOGS=${GC_LOGS:-"-Xlog:gc:${LOG_DIR}/wdqs-updater_jvm_gc.%p.log \
         -Xlog:gc* \
         -XX:+UnlockExperimentalVMOptions \
         -XX:G1NewSizePercent=20 \
         -XX:+ParallelRefProcEnabled"}
else
    GC_LOGS=${GC_LOGS:-"-Xloggc:${LOG_DIR}/wdqs-updater_jvm_gc.%p.log \
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

while getopts h:c:n:l:t:sSNv option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    n) NAMESPACE=${OPTARG};;
    l) LANGS=${OPTARG};;
    t) TMO=${OPTARG};;
    s) SKIPSITE=1;;
    S) NO_SERVICE=1;;
    N) NOEXTRA=1;;
    v) VERBOSE_LOGGING="true";;
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
# if not running as a service, use the default log file
if [ ! -z "$NO_SERVICE" ]; then
    LOG_CONFIG=""
fi
if [ ! -z "$VERBOSE_LOGGING" ]; then
    LOG_OPTIONS="-Dlogback.configurationFile=logback-verbose.xml"
elif [ ! -z "$LOG_CONFIG" ]; then
    LOG_OPTIONS="-Dlogback.configurationFile=${LOG_CONFIG}"
fi
# No extra options - for running secondary updaters, etc.
if [ ! -z "$NOEXTRA" ]; then
	EXTRA_JVM_OPTS=""
	GC_LOGS=""
fi

CP=`dirname $BASH_SOURCE`/lib/wikidata-query-tools-*-jar-with-dependencies.jar
MAIN=org.wikidata.query.rdf.tool.Update
SPARQL_URL=$HOST/$CONTEXT/namespace/$NAMESPACE/sparql
echo "Updating via $SPARQL_URL"
exec java -cp ${CP} ${MEMORY} ${GC_LOGS} ${LOG_OPTIONS} ${EXTRA_JVM_OPTS} \
     ${TIMEOUT_ARG} ${UPDATER_OPTS} \
     ${MAIN} ${ARGS} --sparqlUrl ${SPARQL_URL} "$@"
