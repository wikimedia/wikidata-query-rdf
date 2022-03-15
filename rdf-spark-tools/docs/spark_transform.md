# `spark.transform` package

Contains code to perform transformations on data using spark. Data can be structured data (Wikidata, Wikimedia Commons)
or queries related to these data.

# Package overview

## `spark.transform.structureddata.dumps`

TBD

## `spark.transform.structureddata.subgraphs`

This package contains scala-spark code to

- Identify subgraphs in Wikidata
- Map every item and triple in Wikidata to a subgraph(s). The result is then called subgraph-mapping.

The core of this analysis is the idea of dividing Wikidata into non-disjoint subgraphs. Subgraphs in wikidata give us an
idea about what Wikidata is composed of, what topics are covered the most, and what kinds of data are queried the most
by users.

### Subgraph

A subgraph is defined as a collection of items and their related triples such that all of these items
are `instance of (P31)`
of the same entity. For example, given the triples:

- `Q123 P31 Q900`
- `Q456 P31 Q900`

`Q123` and `Q456` are considered part of the `Q900` subgraph.

Find more explanation and analysis in the following pages:

- [Overview of Wikidata](https://wikitech.wikimedia.org/wiki/User:AKhatun/Wikidata_Basic_Analysis)
- [Wikidata subgraph analysis](https://wikitech.wikimedia.org/wiki/User:AKhatun/Wikidata_Subgraph_Analysis)
    - [Scholarly articles analysis](https://wikitech.wikimedia.org/wiki/User:AKhatun/Wikidata_Scholarly_Articles_Subgraph_Analysis)
- [Analysis on vertical data](https://wikitech.wikimedia.org/wiki/User:AKhatun/Wikidata_Vertical_Analysis)

### Entry Points

`SubgraphMappingLauncher.scala`: Entry point for subgraph mapping. It identifies subgraphs in Wikidata and maps items
and triples in Wikidata to be related to one or more subgraphs. Calls `SubgraphMapper.scala`.

- **Input:**
  - wikidataTriples: Entire Wikidata.
  - minItems: The minimum number of items a subgraph must contain to be considered a top subgraph.
- **Output:**
  - allSubgraphs: List of all subgraphs and the number of items each subgraph has.
  - topSubgraphItems: Mapping of items in Wikidata with the top subgraphs.
  - topSubgraphTriples: Mapping of triples in Wikidata with the top subgraphs.

## `spark.transform.queries.sparql`

This package contains scala-spark code to process SPARQL queries using Apache Jena ARQ.

## `spark.transform.queries.subgraphsqueries`

This package contains scala-spark code to identify which subgraphs are accessed by the WDQS SPARQL queries. The results
are then called subgraph-query mapping, or subgraph-queries.

### Subgraph Queries

SPARQL queries done through the WDQS can access the whole Wikidata. When doing subgraph analysis, we would like to find
out which of the queries access each of the subgraphs. This gives us various information about the subgraphs' usage
including number of query, query time taken, types of query, etc.

**A query can access multiple subgraphs.** The way to define what subgraph a query is accessing is based on some rough
heuristics. In short, if a query fulfils any of the following conditions, it is considered accessing that subgraph.

- Does the query contain the Q-id of the subgraph (e.g `Q900` from above)?
- Does the query contain Q-id of any of the items that are `instance of (P31)` the subgraph?
- Does the query contain predicates that occur 99% times in the subgraph?
- Does the query contain URIs that occur 99% times in the subgraph (as subject/object)?
- Does the query contain literals that occur 99% times in the subgraph?

*The threshold `99%` can be changed if required.

Find more explanation and analysis in the following pages:

- Overview of SPARQL queries
    - [SPARQL query analysis](https://wikitech.wikimedia.org/wiki/User:Joal/WDQS_Queries_Analysis)
    - [Traffic analysis](https://wikitech.wikimedia.org/wiki/User:Joal/WDQS_Traffic_Analysis)
    - [Triples analysis](https://wikitech.wikimedia.org/wiki/User:AKhatun/WDQS_Triples_Analysis)
- [Subgraph query analysis](https://wikitech.wikimedia.org/wiki/User:AKhatun/Wikidata_Subgraph_Query_Analysis)
    - [Scholarly articles analysis](https://wikitech.wikimedia.org/wiki/User:AKhatun/Wikidata_Scholarly_Articles_Subgraph_Analysis)

### Entry Points

`SubgraphQueryMappingLauncher.scala`: Entry point to map WDQS SPARQL queries to subgraphs, i.e which subgraphs are
accessed by each of the queries. Note that a given query can access multiple subgraphs at once.
Calls `SubgraphQueryMapper.scala`.

- **Input:**
  - wikidataTriples: Entire Wikidata
  - processedQueries: Parsed SPARQL queries
  - topSubgraphTriples: Map of triples to subgraphs.
  - topSubgraphItems : Map of items to subgraphs.
  - filteringLimit: If the triples of a subgraph have `filteringLimit` percent of nodes (items, predicates, URIs), then
    those nodes are considered part of that subgraph. These nodes are then matched with queries to identify and map
    subgraph queries.
- **Output:**
  - queryMapping: Table of mapping between SPARQL queries and the subgraph it accesses, along with the reason for the
    match to occur (boolean).
  - subgraphQItemsMatchInQuery: List of queries and the item that caused it to match to a subgraph. Used to get some
    metrics.
  - subgraphPredicatesMatchInQuery: List of queries and the predicates that caused it to match to a subgraph. Used to
    get some metrics.
  - subgraphUrisMatchInQuery: List of queries and the URIs that caused it to match to a subgraph. Used to get some
    metrics.

## Data Flow (Subgraphs and Subgraph Queries)

### DataFlow Diagram

![DataFlow Diagram](rdf-spark-tools/docs/subgraph_dataflow_diagram.png)

### Dependency Diagram

![Dependency Diagram](rdf-spark-tools/docs/subgraph_dependency_diagram.png)