#!/usr/bin/env bash

START=1
END=100000
LOCATION=`pwd`
FORMAT=wikidump-%09d.ttl.gz
CHUNK=100000
SKIPSITE=
LANGS=

while getopts d:f:l:s option
do
  case "${option}"
  in
    d) LOCATION=${OPTARG};;
	f) FROM=${OPTARG};;
	l) LANGS=${OPTARG};;
	s) SKIPSITE=1;;
  esac
done

# allow extra args
shift $((OPTIND-1))

if [ -z "$FROM" -o ! -f "$FROM" ]
then
  echo "Usage: $0 -f <dumpfile> [-d <directory>] [-l languages]"
  exit 1
fi
if [ -z "$LANGS" ]; then
	ARGS=
else 
	ARGS="--labelLanguage $LANGS --singleLabel $LANGS"
fi

if [ ! -z "$SKIPSITE" ]; then
	ARGS="$ARGS --skipSiteLinks"
fi

CP=lib/wikidata-query-tools-*-jar-with-dependencies.jar
MAIN=org.wikidata.query.rdf.tool.Munge
java -cp $CP $MAIN --from $FROM --to $LOCATION/$FORMAT $ARGS --chunkSize $CHUNK "$@"
	
