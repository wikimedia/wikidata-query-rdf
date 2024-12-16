#!/bin/bash
DIR=`dirname $0`
ENDPOINT=https://query.wikidata.org/
ESC_SEQ="\x1b["
COL_RESET=$ESC_SEQ"39m"
COL_RED=$ESC_SEQ"31m"
COL_GREEN=$ESC_SEQ"32m"
COL_LPURP=$ESC_SEQ"1;35m"
KEEP=no

function gethelp {
	echo "Usage: $0 [-s ENDPOINT] [-k] [scripts...]"
	echo "Examples:"
	echo -e "\tRun tests on query.wikidata.org: test.sh"
	echo -e "\tRun tests on query-main.wikidata.org: test.sh -d wikidata_main -s https://query-main.wikidata.org"
	echo -e "\tRun tests on query-scholarly.wikidata.org: test.sh -d scholarly_articles -s https://query-scholarly.wikidata.org"
	exit 0
}

SUBDIR=.
while getopts s:d:kh option
do
  case "${option}"
  in
    h) gethelp;;
    s) ENDPOINT=${OPTARG};;
    k) KEEP=yes;;
    d) SUBDIR=${OPTARG};;
  esac
done

# allow extra args
shift $((OPTIND-1))

function runtest {
	fname=$(basename -s .sparql $1)
	out=/tmp/querytest.$fname.$$
	curl -G --data-urlencode query@$1 --data format=json --data random=$$ $ENDPOINT/sparql -s > $out
	if [ -z $out ]; then
		failed
	else
		if [ -f $fname.result ]; then
			lookup=`cat $fname.result`
		else
			lookup='"results" : {'
		fi
		if grep -q -l -F "$lookup" $out; then
			passed
		else
			failed
		fi
	fi
	if [ $KEEP == "no" ]; then
		rm $out
	fi
}

function passed {
	echo -e "$COL_GREEN PASSED$COL_RESET"
}

function failed {
	echo -e "$COL_RED FAILED$COL_RESET"
}

QUERY_DIR=$DIR/$SUBDIR
if [ ! -d $QUERY_DIR ]; then
	echo "$QUERY_DIR not found"
	exit 1
fi

cd $DIR/$SUBDIR

if [ "x$1" != "x" ]; then
	scripts="$1"
else
	scripts=`ls *.sparql`
fi

for tstname in $scripts; do
	echo -n Running $tstname
	runtest $tstname
done

echo -en "\nTotal triples: \n$COL_LPURP"
curl -s -XPOST --data-urlencode "query=SELECT (COUNT(*) AS ?C) { ?s ?p ?o . } LIMIT 1" $ENDPOINT/sparql?format=json | jq -r .results.bindings[0].C.value
echo -en "$COL_RESET"
