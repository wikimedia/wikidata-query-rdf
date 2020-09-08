#!/usr/bin/env bash

DUMP_LIST=${DUMP_LIST:-"https://noc.wikimedia.org/conf/dblists/categories-rdf.dblist"}
DIR=${DIR:-`dirname $0`}
COMMAND="$1"

if [ -f $DUMP_LIST ]; then
	fetch="cat"
else
	fetch="curl --silent --fail -XGET"
fi

shift
STATUS=0

for wiki in $($fetch $DUMP_LIST | grep -v '^#');
do
	echo "Processing $wiki..."
	$DIR/$COMMAND $wiki "$@"
	if [ $? -ne 0 ]; then
		echo "ERROR:$COMMAND - Failed for $wiki, moving on to the next entry..." 1>&2
		STATUS=1
	fi
done

exit $STATUS