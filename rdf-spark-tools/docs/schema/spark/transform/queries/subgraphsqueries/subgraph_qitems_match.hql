CREATE TABLE IF NOT EXISTS `subgraph_qitems_match` (
    `id`                          string  COMMENT 'ID of the SPARQL query',
    `subgraph`                    string  COMMENT 'URI of the subgraph the query accesses',
    `item`                        string  COMMENT 'Item match that caused the query to match with the subgraph'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''