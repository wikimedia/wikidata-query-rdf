CREATE TABLE IF NOT EXISTS `general_subgraph_metrics` (
    `total_items`                        bigint  COMMENT 'Total number of items in Wikidata',
    `total_triples`                      bigint  COMMENT 'Total number of triples in Wikidata',
    `percent_subgraph_item`              double  COMMENT 'Percentage of items covered by the top subgraphs',
    `percent_subgraph_triples`           double  COMMENT 'Percentage of triples covered by the top subgraphs',
    `num_subgraph`                       bigint  COMMENT 'Number of subgraphs in wikidata (using groups of P31)',
    `num_top_subgraph`                   bigint  COMMENT 'Number of top subgraphs (has at least `minItems` items)',
    `subgraph_size_percentiles`          array<double>  COMMENT 'List of values containing the percentile values from 0.1 to 0.9 of the size of subgraphs (in triples)',
    `subgraph_size_mean`                 double  COMMENT 'Mean of the size (in triples) of all subgraphs'
)
PARTITIONED BY (
    `date` string,
    `wiki` string
)
STORED AS PARQUET
LOCATION ''