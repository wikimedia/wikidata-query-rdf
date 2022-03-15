CREATE TABLE IF NOT EXISTS `all_subgraphs` (
    `subgraph`                    string  COMMENT 'URI of subgraphs in wikidata',
    `count`                       string  COMMENT 'Number of items in the subgraph'
)
PARTITIONED BY (
    `date` string,
    `wiki` string
)
STORED AS PARQUET
LOCATION ''