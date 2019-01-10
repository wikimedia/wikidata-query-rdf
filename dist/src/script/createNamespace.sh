#/bin/bash
set -e

if [ -r /etc/wdqs/vars.sh ]; then
  . /etc/wdqs/vars.sh
fi

# This script can be used to create a new namespace on Blazegraph
HOST=${CATEGORY_ENDPOINT:-"http://localhost:9999"}
URL="$HOST/bigdata/namespace"
CATEGORY=$1

PROPS=`mktemp /tmp/wdqs.XXXXXX` || exit 1
trap "rm -f $PROPS" EXIT

sed -e "s/{NAMESPACE}/$CATEGORY/" < default.properties > $PROPS
cat $PROPS
curl --silent --show-error -XPOST $URL --data-binary @$PROPS  -H 'Content-type: text/plain'
echo
