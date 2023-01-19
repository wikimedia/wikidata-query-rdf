#!/bin/bash

DIR=${DIR:-`dirname $0`}
cd $DIR/..
if [ "$1x" = "-dx" -o "$1x" = "--debugx" ]; then
	shift
	export MAVEN_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
	./mvnw -pl tools jetty:run "$@"
else
	./mvnw -pl tools jetty:run "$@"
fi
