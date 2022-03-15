CREATE TABLE IF NOT EXISTS `general_subgraph_query_metrics` (
    `total_subgraph_query_count`        bigint  COMMENT 'Number of queries that access the top subgraphs',
    `ua_subgraph_dist`                  array<struct<subgraph_count:bigint, ua_count:bigint> >  COMMENT 'List of the number of user-agents accessing how many subgraphs each',
    `query_subgraph_dist`               array<struct<subgraph_count:bigint, query_count:bigint> >  COMMENT 'List of the number of queries accessing how many subgraphs at once',
    `query_time_class_subgraph_dist`    array<
                                            struct<
                                               subgraph_count: string,
                                               query_time_class: map<string, bigint>
                                            >
                                        >  COMMENT 'Query time class distribution of queries that access 1,2,3,4,4+,n/a subgraphs. Here `n/a` means a query does not access any of the top subgraphs'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''