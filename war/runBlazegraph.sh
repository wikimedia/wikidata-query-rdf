#!/bin/bash

DIR=${DIR:-`dirname $0`}
cd $DIR/..
if [ "$1x" = "-dx" -o "$1x" = "--debugx" ]; then
	shift
	mvnDebug -pl tools jetty:run "$@"
else
	mvn -pl tools jetty:run "$@"
fi
