#!/bin/bash
DIR=`dirname $0`
ENDPOINT=https://query.wikidata.org/
ESC_SEQ="\x1b["
COL_RESET=$ESC_SEQ"39m"
COL_RED=$ESC_SEQ"31m"
COL_GREEN=$ESC_SEQ"32m"
KEEP=no

function gethelp {
	echo "Usage: $0 [-s ENDPOINT] [-k] [scripts...]"
	exit 0
}

while getopts s:kh option
do
  case "${option}"
  in
    h) gethelp;;
    s) ENDPOINT=${OPTARG};;
    k) KEEP=yes;;
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

cd $DIR

if [ "x$1" != "x" ]; then
	scripts="$1"
else
	scripts=`ls *.sparql`
fi

for tstname in $scripts; do
	echo -n Running $tstname
	runtest $tstname
done

