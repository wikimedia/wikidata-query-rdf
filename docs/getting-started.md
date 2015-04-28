# Getting started

Clone the project:

```
$ git clone https://gerrit.wikimedia.org/r/wikidata/query/rdf wikidata-query-rdf
```

Build it:

```
$ cd wikidata-query-rdf
$ mvn package
```

The distributable package can be found in *./dist/target/service-VERSION-dist.zip*:

```
$ cd dist/target
$ unzip service-*-dist.zip
$ cd service-*/
```

Unzipping the package, we find a customized Blazegraph, Jetty launcher, launch scripts, and configuration:

```
.
├── blazegraph
├── jetty-runner-9.2.9.v20150224.jar
├── lib
├── runBlazegraph.sh
├── runUpdate.sh
└── RWStore.properties
```

Optionally modify *RWStore.properties*.

Launch Blazegraph:

```
$ ./runBlazegraph.sh
```

Blazegraph is now running on *localhost:9999* with a fresh database.

To populate the database from Wikidata, open a second terminal and run the updater with the *wdq* Blazegraph namespace:

```
$ ./runUpdate.sh -n wdq
```

This will take a long time to complete, but can be interrupted and resumed at any time.
