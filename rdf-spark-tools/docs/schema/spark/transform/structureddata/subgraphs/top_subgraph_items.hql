CREATE TABLE IF NOT EXISTS `top_subgraph_items` (
    `subgraph`                    string  COMMENT 'URI of subgraphs in wikidata',
    `item`                        string  COMMENT 'Item belonging to corresponding subgraph'
)
PARTITIONED BY (
    `snapshot` string,
    `wiki` string
)
STORED AS PARQUET
LOCATION ''