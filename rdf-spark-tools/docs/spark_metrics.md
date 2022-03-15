# `spark.metrics` package

Contains code to extract metrics from data using spark. Data can be structured data (Wikidata, Wikimedia Commons)
or queries related to these data.

## Package overview

## `spark.metrics.queries.subgraphs`

This package contains scala-spark code to extract metrics related to the subgraph queries. This includes metrics such as
number of queries, query time, ua analysis etc per subgraph.

## `spark.metrics.queries.subgraphpairs`

This package contains scala-spark code to extract metrics related to subgraph-pairs queries (number of queries per
subgraph-pair).

## `spark.metrics.subgraphs.general`

This package contains scala-spark code to extract aggregated metrics related to overall Wikidata and its subgraphs
(size/item distributions of subgraphs).

## `spark.metrics.subgraphs.detailed`

This package contains scala-spark code to extract metrics related to subgraph, both per subgraph metrics and
subgraph-pair metrics.

The core of this analysis is the idea of dividing Wikidata into non-disjoint subgraphs. Subgraphs in wikidata give us an
idea about what Wikidata is composed of, what topics are covered the most, and what kinds of data are queried the most
by users.

## Entry Points

`SubgraphMetricsLauncher.scala`: Entry point to extract various metrics related to subgraphs and subgraph-queries. Based
on the command issued, this file runs 4 different jobs to collect and calculate metrics which are then saved in tabular
format. The commands are as follows:

1. general-subgraph-metrics: Calls `GeneralSubgraphMetricsExtractor.scala`.
    - **Input**:
        - wikidataTriples: Entire Wikidata
        - allSubgraphs: List of all subgraphs and the number of items each subgraph has.
        - topSubgraphItems : Map of items to subgraphs.
        - topSubgraphTriples: Map of triples to subgraphs.
    - **Output**:
        - generalSubgraphMetrics: Table holding aggregate metrics on Wikidata items, triples and subgraph distribution.
2. detailed-subgraph-metrics: Calls `DetailedSubgraphMetricsExtractor.scala`.
    - **Input**:
        - minItems: Number of top entries to save for various analysis.
        - generalSubgraphMetrics: Table holding aggregate metrics on Wikidata items, triples and subgraph distribution.
        - allSubgraphs: List of all subgraphs and the number of items each subgraph has.
        - topSubgraphItems : The output from subgraphMapping. Map of items to subgraphs.
        - topSubgraphTriples: The output from subgraphMapping. Map of triples to subgraphs.
    - **Output**:
        - perSubgraphMetrics: Table holding metrics per subgraph related to items, triples, predicates, and subgraph
          connection to Wikidata.
        - subgraphPairMetrics: Table holding information about items and predicates common to subgraph pairs and their
          connection through triples.
3. query-metrics: Calls `QueryMetricsExtractor.scala`.
    - **Input**:
        - eventQueries: All WDQS SPARQL queries (unprocessed and unfiltered)
        - processedQueries: Parsed SPARQL queries
        - subgraphQueries: Table of mapping between SPARQL queries and the subgraph it accesses, along with the reason
          for the match to occur (boolean).
        - subgraphQItemsMatchInQuery: List of queries and the item that caused it to match to a subgraph. Used to get
          some metrics.
        - subgraphPredicatesMatchInQuery: List of queries and the predicates that caused it to match to a subgraph. Used
          to get some metrics.
        - subgraphUrisMatchInQuery: List of queries and the URIs that caused it to match to a subgraph. Used to get some
          metrics.
        - topN: Number of top entries to save for various analysis.
    - **Output**:
        - generalQueryMetrics: Table holding aggregate query metrics about counts, query-time, and user-agents and
          subgraph query count distribution and query-time
        - generalSubgraphQueryMetrics: Table holding aggregate subgraph query metrics about query-count, query-time,
          user-agent distribution
        - perSubgraphQueryMetrics: Table holding per subgraph query metrics about query-count, query-time, user-agent,
          top query to subgraph match reasons (items, predicates, uris)
4. subgraph-pair-query-metrics: Calls `SubgraphPairQueryMetricsExtractor.scala`.
    - **Input**:
        - subgraphQueries: Table of mapping between SPARQL queries and the subgraph it accesses, along with the reason
          for the match to occur (boolean).
    - **Output**:
        - subgraphPairQueryMetrics: Table holding number of queries accessing subgraph pairs.

## Data Flow (Subgraphs and Subgraph Queries Metrics)

### DataFlow Diagram

![DataFlow Diagram](rdf-spark-tools/docs/subgraph_dataflow_diagram.png)

### Dependency Diagram

![Dependency Diagram](rdf-spark-tools/docs/subgraph_dependency_diagram.png)