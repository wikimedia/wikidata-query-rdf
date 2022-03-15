CREATE TABLE IF NOT EXISTS `subgraph_predicates_match` (
    `id`                          string  COMMENT 'ID of the SPARQL query',
    `subgraph`                    string  COMMENT 'URI of the subgraph the query accesses',
    `predicate_code`              string  COMMENT 'Wikidata predicates present in queries that are part of the subgraph (causing the match)'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''