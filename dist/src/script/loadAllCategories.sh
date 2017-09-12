#!/bin/bash
DUMP_LIST=${DUMP_LIST:-"https://noc.wikimedia.org/conf/categories-rdf.dblist"}
DIR=${DIR:-`dirname $0`}

if [ -f $DUMP_LIST ]; then
	fetch="cat"
else
	fetch="curl -s -XGET"
fi

$fetch $DUMP_LIST | while read wiki; do
	echo "Loading $wiki..."
	$DIR/loadCategoryDump.sh $wiki
done 