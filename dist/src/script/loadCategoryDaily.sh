#!/usr/bin/env bash
set -e

if [ -r /etc/wdqs/vars.sh ]; then
  . /etc/wdqs/vars.sh
fi
if [ -r /etc/wdqs/gui_vars.sh ]; then
  . /etc/wdqs/gui_vars.sh
fi

SOURCE=${SOURCE:-"https://dumps.wikimedia.org/other/categoriesrdf/daily"}
DATA_DIR=${DATA_DIR:-"/srv/wdqs"}
DUMPS_DIR="${DATA_DIR}/dumps"
HOST=${CATEGORY_ENDPOINT:-"http://localhost:9999"}
CONTEXT=bigdata
NAMESPACE=$(cat $ALIAS_FILE | grep categories | cut -d' ' -f2 | cut -d ';' -f1)
WIKI=$1
TS=$2
PREFIX=$3

if [ -z "$WIKI" ]; then
	echo "Use: $0 WIKI-NAME [TS] [PREFIX]"
	exit 1
fi

if [ -z "$TS" ]; then
	TSURL="$SOURCE/lastdump/$WIKI-daily.last"
	TS=$(curl --silent --fail -XGET "$TSURL" | cut -c1-8)
	if [ -z "$TS" ]; then
		echo "Could not load timestamp from $TSURL"
		exit 1
	fi
fi

FILENAME="$PREFIX$WIKI-$TS-daily.sparql.gz"
URL="$SOURCE/$TS/$FILENAME"
curl --silent --fail -XGET "$URL" -o "$DUMPS_DIR/$FILENAME"
if [ ! -s $DUMPS_DIR/$FILENAME ]; then
	echo "Could not download $URL"
	exit 1
fi
gunzip -dc $DUMPS_DIR/$FILENAME | curl --silent --show-error -XPOST -H 'Content-type:application/sparql-update' --data-binary @- $HOST/$CONTEXT/namespace/$NAMESPACE/sparql
