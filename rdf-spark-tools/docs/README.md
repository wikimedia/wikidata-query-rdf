# rdf-spark-tools

Module to work on structured data (Wikidata, Wikimedia Commons).

# Package structure

<pre><code>
<b>org.wikidata.query.rdf</b>
        ├── <b>spark</b>
        │   ├── <b>transform</b>: Transformations on data using spark. See more in <b>spark_transform.md</b>.
        │   │   ├── <b>queries</b>: Transformations on SPARQL queries related to structured data
        │   │   │   ├── <b>sparql</b>: WDQS SPARQL query processing
        │   │   │   └── <b>subgraphsqueries</b>: WDQS SPARQL query subgraph mapping
        │   │   └── <b>structureddata</b>: Transformations on the structured data themselves
        │   │       ├── <b>dumps</b>
        │   │       └── <b>subgraphs</b>: Wikidata subgraph mapping
        │   └── <b>metrics</b>: Metrics extraction from structured data using spark. See more in <b>spark_metrics.md</b>.
        │       ├── <b>queries</b>: SPARQL query metrics extraction
        │       │   ├── <b>subgraphs</b>: Subgraph query related metrics
        │       │   └── <b>subgraphpairs</b>: Subgraph pair related query metrics
        │       └── <b>subgraphs</b>: Wikidata subgraph metrics extraction
        │           ├── <b>general</b>: Basic subgraph metrics
        │           └── <b>detailed</b>: Detailed (per subgraph and per subgraph-pair) metrics
        └── <b>updater.reconcile</b>
</code></pre>

# Documentation

You can find documentation in the `docs` folder. It contains:

- A [`README.md`](rdf-spark-tools/docs/README.md) file, showing the basic structure of the project and what each package is dedicated to.
- A [`spark_transform.md`](rdf-spark-tools/docs/spark_transform.md) file, describing the code architecture, jobs, and information about the
  input and output data of each sub-package within the transform package.
- A [`spark_metrics.md`](rdf-spark-tools/docs/spark_metrics.md) file, describing the code architecture, jobs, and information about the
  input and output data of each sub-package within the metrics package.
- `subgraph_dataflow_diagram.png` showing the inputs and outputs of all the subgraph related (both transformations and
  metrics) processes in a visual format.
- `subgraph_dependency_diagram.png` is a simplified diagram showing which process is dependent on the completion of
  which ones (subgraph related transformations and metrics).
- A [`schema`](rdf-spark-tools/docs/schema) folder, which contains schemas of all the tables saved through various jobs within this
  package.
- A [`query_recorder.md`](rdf-spark-tools/docs/query_recorder.md) file, demonstrating how to collect sparql query results and compare them.