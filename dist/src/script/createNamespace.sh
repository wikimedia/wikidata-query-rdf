#!/bin/bash
set -e

# This script can be used to create a new namespace on Blazegraph
ENDPOINT="http://localhost:9999"
if [ "x$2" != "x" ]; then
	ENDPOINT=$2
fi
URL="$ENDPOINT/bigdata/namespace"

NAMESPACE=$1

PROPS=`mktemp /tmp/wdqs.XXXXXX` || exit 1
trap "rm -f $PROPS" EXIT

if [ "x$3" != "x" ]; then
  sed -e "s/\.$4\./.$NAMESPACE./" < "$3" > $PROPS

else
  sed -e "s/{NAMESPACE}/$NAMESPACE/" < default.properties > $PROPS
fi
cat $PROPS
curl --silent --show-error -XPOST $URL --data-binary @$PROPS  -H 'Content-type: text/plain'
echo
