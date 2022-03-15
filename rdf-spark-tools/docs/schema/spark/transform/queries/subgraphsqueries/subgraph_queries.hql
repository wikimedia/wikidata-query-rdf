CREATE TABLE IF NOT EXISTS `subgraph_queries` (
    `id`                string   COMMENT 'ID of the SPARQL query',
    `subgraph`          string   COMMENT 'URI of the subgraph the query accesses',
    `qid`               boolean  COMMENT 'Whether the subgraph-query match was through the subgraphs Qid',
    `item`              boolean  COMMENT 'Whether the subgraph-query match was through an item',
    `predicate`         boolean  COMMENT 'Whether the subgraph-query match was through a predicate',
    `uri`               boolean  COMMENT 'Whether the subgraph-query match was through a URI',
    `literal`           boolean  COMMENT 'Whether the subgraph-query match was through a literal'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''