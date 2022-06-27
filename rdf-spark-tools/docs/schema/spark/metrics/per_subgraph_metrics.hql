CREATE TABLE IF NOT EXISTS `per_subgraph_metrics` (
    `subgraph`                          string  COMMENT 'URI of the subgraphs in wikidata',
    `item_count`                        bigint  COMMENT 'Total number of items/entities in subgraph',
    `triple_count`                      bigint  COMMENT 'Total number of triples in subgraph',
    `predicate_count`                   bigint  COMMENT 'Total number of distinct predicates in subgraph',
    `item_percent`                      double  COMMENT 'Percent of items/entities in subgraph compared to total items in Wikidata',
    `triple_percent`                    double  COMMENT 'Percent of triples in subgraph compared to total triples in Wikidata',
    `density`                           double  COMMENT 'Average triples per item, represents density of subgraphs',
    `item_rank`                         bigint  COMMENT 'Rank of the subgraph by number of items it contains in descending order',
    `triple_rank`                       bigint  COMMENT 'Rank of the subgraph by number of triples it contains in descending order',
    `triples_per_item_percentiles`      array<double>  COMMENT 'List of 0.1 to 0.9 percentile of triples per item in each subgraph',
    `triples_per_item_mean`             double  COMMENT 'Mean of triples per item in each subgraph',
    `num_direct_triples`                bigint  COMMENT 'Number of direct triples (triples that are not statements)',
    `num_statements`                    bigint  COMMENT 'Number of statements (wikidata.org/prop/)',
    `num_statement_triples`             bigint  COMMENT 'Number of triples in the full statements (everything within the statements)',
    `predicate_counts`                  map<string, bigint>  COMMENT 'Map of predicates and the number of its occurrences in the subgraph',
    `subgraph_to_WD_triples`            bigint  COMMENT 'Number of triples connecting this subgraph to other subgraphs',
    `WD_to_subgraph_triples`            bigint  COMMENT 'Number of triples connecting other subgraphs to this subgraph'
)
PARTITIONED BY (
    `snapshot` string,
    `wiki` string
)
STORED AS PARQUET
LOCATION ''