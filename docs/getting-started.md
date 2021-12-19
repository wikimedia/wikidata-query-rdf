# Getting started

## Clone the project

```
$ git clone https://gerrit.wikimedia.org/r/wikidata/query/rdf wikidata-query-rdf
```

## Build it

```
$ cd wikidata-query-rdf
$ mvn package
```

The distributable package can be found in *./dist/target/service-VERSION-dist.tar.gz*:

```
$ cd dist/target
$ tar xf service-*-dist.tar.gz
$ cd service-*/
```

Extracting the package, we find a customized Blazegraph, Jetty launcher, launch scripts, and configuration:

```
.
├── blazegraph-service-0.1.0-dist.war
├── docs
├── jetty-runner-9.2.9.v20150224.jar
├── lib
├── munge.sh
├── runBlazegraph.sh
├── runUpdate.sh
└── RWStore.properties
```

Optionally modify *RWStore.properties*.

## Launch Blazegraph

```
$ ./runBlazegraph.sh
```

Blazegraph is now running on *localhost:9999* with a fresh database.

## Load the dump

* Create `data` directory:
```
$ mkdir data
```
* Download the dump file from https://dumps.wikimedia.org/wikidatawiki/entities/ (for subdirectory `20150427` the filename will be something like `wikidata-20150427-all-BETA.ttl.gz`) into the `data` directory.
* Pre-process the dump with Munger utility:
```
$ mkdir data/split
$ ./munge.sh -f data/wikidata-20150427-all-BETA.ttl.gz -d data/split -l en -s
```
The option `-l en` only imports English labels.  The option `-s` skips the sitelinks, for smaller storage and better performance.
If you need labels in other languages, either add them to the list - `-l en,de,ru` - or skip the language option altogether. If you need sitelinks, remove the `-s` option.

* The Munger will produce a lot of data files named like `wikidump-000000001.ttl.gz`, `wikidump-000000002.ttl.gz`, etc. To load these files, you can use the following script:
```
$ ./loadRestAPI.sh -n wdq -d `pwd`/data/split
```

This will load the data files one by one into the Blazegraph data store. Note that you need `curl` to be installed for it to work.

You can also load specific files:
```
$ ./loadRestAPI.sh -n wdq -d `pwd`/data/split/wikidump-000000001.ttl.gz
```

## Run updater

To update the database from Wikidata fresh edits, open a second terminal and run the updater with the *wdq* Blazegraph namespace:

```
$ ./runUpdate.sh -n wdq
```

The updater is designed to run constantly, but can be interrupted and resumed at any time. Note that if you loaded an old dump, or did not load any dump at all, it may require very long time for the data to be full synchronized, as updater only picks up recently edited items.
Use the same set of language/skip options as in the `munge.sh` script, e.g. `-l en -s`.

## Run queries

The REST query endpoint is located at *http://localhost:9999/bigdata/namespace/wdq/sparql*, read more at http://wiki.blazegraph.com/wiki/index.php/NanoSparqlServer#REST_API.

Examples of SPARQL queries can be found at https://www.mediawiki.org/wiki/Wikibase/Indexing/SPARQL_Query_Examples . For WDQ query translation, use http://tools.wmflabs.org/wdq2sparql/w2s.php (choose "Wikidata RDF Syntax").
