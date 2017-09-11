#!/bin/bash
if [ -r /etc/wdqs/vars.sh ]; then
  . /etc/wdqs/vars.sh
fi

SOURCE=${SOURCE:-"https://dumps.wikimedia.org/other/categoriesrdf"}
DATA_DIR=${DATA_DIR:-"/srv/wdqs"}
HOST=http://localhost:9999
CONTEXT=bigdata
NAMESPACE=categories
WIKI=$1

if [ -z "$WIKI" ]; then
	echo "Use: $0 WIKI-NAME"
	exit 1
fi

TS=$(curl -s -XGET $SOURCE/lastdump/$WIKI-categories.last)
if [ -z "$TS" ]; then
	echo "Could not load timestamp"
	exit 1
fi
FILENAME=$WIKI-$TS-categories.ttl.gz
curl -s -XGET $SOURCE/$TS/$FILENAME -o $DATA_DIR/$FILENAME
if [ ! -s $DATA_DIR/$FILENAME ]; then
	echo "Could not download $FILENAME"
	exit 1
fi	
curl -XPOST --data-binary update="LOAD <file://$DATA_DIR/$FILENAME>" $HOST/$CONTEXT/namespace/$NAMESPACE/sparql