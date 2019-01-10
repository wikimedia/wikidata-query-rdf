#/bin/bash
set -e

# This script can be used to create a new namespace on Blazegraph
ENDPOINT="http://localhost:9999"
if [ "x$2" != "x" ];
	ENDPOINT=$2
fi
URL="$HOST/bigdata/namespace"

CATEGORY=$1

PROPS=`mktemp /tmp/wdqs.XXXXXX` || exit 1
trap "rm -f $PROPS" EXIT

sed -e "s/{NAMESPACE}/$CATEGORY/" < default.properties > $PROPS
cat $PROPS
curl --silent --show-error -XPOST $URL --data-binary @$PROPS  -H 'Content-type: text/plain'
echo
