CREATE TABLE IF NOT EXISTS `top_subgraph_triples` (
    `subgraph`                    string  COMMENT 'URI of subgraphs in wikidata',
    `item`                        string  COMMENT 'Item belonging to corresponding subgraph',
    `subject`                     string  COMMENT 'Subject of the triple',
    `predicate`                   string  COMMENT 'Predicate of the triple',
    `object`                      string  COMMENT 'Object of the triple',
    `predicate_code`              string  COMMENT 'Last suffix of the predicate of the triple (i.e P123, rdf-schema#label etc)'
)
PARTITIONED BY (
    `snapshot` string,
    `wiki` string
)
STORED AS PARQUET
LOCATION ''