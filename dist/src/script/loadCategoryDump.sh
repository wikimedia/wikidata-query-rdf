#!/usr/bin/env bash
set -e

if [ -r /etc/wdqs/vars.sh ]; then
  . /etc/wdqs/vars.sh
fi

SOURCE=${SOURCE:-"https://dumps.wikimedia.org/other/categoriesrdf"}
DATA_DIR=${DATA_DIR:-"/srv/wdqs"}
HOST=${CATEGORY_ENDPOINT:-"http://localhost:9999"}
CONTEXT=bigdata
NAMESPACE=categories
WIKI=$1

if [ "x$2" != "x" ]; then
	NAMESPACE=$2
fi

if [ -z "$WIKI" ]; then
	echo "Use: $0 WIKI-NAME"
	exit 1
fi

TS=$(curl --silent --fail -XGET $SOURCE/lastdump/$WIKI-categories.last | cut -c1-8)
if [ -z "$TS" ]; then
	echo "Could not load timestamp from $SOURCE/lastdump/$WIKI-categories.last"
	exit 1
fi
FILENAME=$WIKI-$TS-categories.ttl.gz
curl --silent --fail -XGET $SOURCE/$TS/$FILENAME -o $DATA_DIR/$FILENAME
if [ ! -s $DATA_DIR/$FILENAME ]; then
	echo "Could not download $FILENAME"
	exit 1
fi
curl --silent --show-error -XPOST --data-binary update="LOAD <file://$DATA_DIR/$FILENAME>" $HOST/$CONTEXT/namespace/$NAMESPACE/sparql
