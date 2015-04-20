# Wikibase RDF Query Service

## Installation

To create a distributable artifact, tell Maven to package the parent project:

```
cd <wikidata/query/rdf>
mvn package
```

This handles all the prerequisites, and creates a distributable *.zip* package in *./dist/target/service-&lt;version&gt;zip*.  This package contains Blazegraph, Wikidata RDF Query extensions and libraries, and scripts to run both Blazegraph and the updater tool:

```
service-<version>
├── blazegraph/
├── lib/
├── jetty-runner-9.2.9.v20150224.jar
├── runBlazegraph.sh
├── runUpdate.sh
└── RWStore.properties
```

Extract this package somewhere, and fire up Blazegraph:

```bash
$ ./runBlazegraph.sh
```

Optionally extract this package somwhere else, and launch the updater tool:

```bash
$ ./runUpdater.sh
```
