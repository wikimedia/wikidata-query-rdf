#!/usr/bin/env bash

HOST=http://localhost:9999
CONTEXT=bigdata

while getopts h:c:n: option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    n) NAMESPACE=${OPTARG};;
  esac
done

if [ -z "$NAMESPACE" ]
then
  echo "Usage: $0 -n <namespace> [-h <host>] [-c <context>]"
  exit 1
else
  CP=lib/wikidata-query-tools-*-jar-with-dependencies.jar
  MAIN=org.wikidata.query.rdf.tool.Update
  SPARQL_URL=$HOST/$CONTEXT/namespace/$NAMESPACE/sparql
  echo "Updating via $SPARQL_URL"
  java -cp $CP $MAIN --sparqlUrl $SPARQL_URL
fi
