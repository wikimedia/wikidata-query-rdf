#!/bin/bash

HOST=http://localhost:9999
CONTEXT=bigdata
LOAD_PROP_FILE=/tmp/$$.properties
NAMESPACE=wdq

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

export NSS_DATALOAD_PROPERTIES="$SCRIPTPATH/RWStore.properties"

while getopts h:c:n:d: option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    n) NAMESPACE=${OPTARG};;
    d) LOCATION=${OPTARG};;
  esac
done

if [ -z "$LOCATION" ]
then
  echo "Usage: $0 -d <directory>] [-n <namespace>] [-h <host>] [-c <context>]"
  exit 1
fi

#Probably some unused properties below, but copied all to be safe.

cat <<EOT >> $LOAD_PROP_FILE
quiet=false
verbose=0
closure=false
durableQueues=true
#Needed for quads
#defaultGraph=
com.bigdata.rdf.store.DataLoader.flush=false
com.bigdata.rdf.store.DataLoader.bufferCapacity=100000
com.bigdata.rdf.store.DataLoader.queueCapacity=10
#Namespace to load
namespace=$NAMESPACE
#Files to load
fileOrDirs=$LOCATION
#Property file (if creating a new namespace)
propertyFile=$NSS_DATALOAD_PROPERTIES
EOT

echo "Loading with properties..."
cat $LOAD_PROP_FILE

curl -X POST --data-binary @${LOAD_PROP_FILE} --header 'Content-Type:text/plain' $HOST/$CONTEXT/dataloader
#Let the output go to STDOUT/ERR to allow script redirection

rm -f $LOAD_PROP_FILE
