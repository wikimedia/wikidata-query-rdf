CREATE TABLE IF NOT EXISTS `subgraph_pair_metrics` (
    `subgraph1`                                string  COMMENT 'First subgraph of the subgraph pair',
    `subgraph2`                                string  COMMENT 'Second subgraph of the subgraph pair',
    `triples_from_1_to_2`                      bigint  COMMENT 'Number of directed triples that connect from subgraph1 to subgraph2',
    `triples_from_2_to_1`                      bigint  COMMENT 'Number of directed triples that connect from subgraph2 to subgraph1',
    `common_predicate_count`                   bigint  COMMENT 'Number of predicates found in both subgraphs',
    `common_item_count`                        bigint  COMMENT 'Number of items found in both subgraphs',
    `common_item_percent_of_subgraph1_items`   double  COMMENT 'Percent of common items w.r.t total items in subgraph1',
    `common_item_percent_of_subgraph2_items`   double  COMMENT 'Percent of common items w.r.t total items in subgraph2'
)
PARTITIONED BY (
    `date` string,
    `wiki` string
)
STORED AS PARQUET
LOCATION ''