CREATE TABLE IF NOT EXISTS `subgraph_pair_query_metrics` (
    `subgraph1`         string  COMMENT 'First subgraph of the subgraph pair',
    `subgraph2`         string  COMMENT 'Second subgraph of the subgraph pair',
    `query_count`       bigint  COMMENT 'Number of queries that access subgraph1 and subgraph2'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''