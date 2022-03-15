CREATE TABLE IF NOT EXISTS `general_query_metrics` (
    `total_query_count`                 bigint  COMMENT 'Total number of queries',
    `processed_query_count`             bigint  COMMENT 'Total number of queries that were parsed/processed successfully',
    `percent_processed_query`           double  COMMENT 'Percentage of queries that were parsed successfully (w.r.t 200 and 500 queries, except monitoring queries)',
    `distinct_query_count`              bigint  COMMENT 'Number of distinct processed queries',
    `percent_query_repeated`            double  COMMENT 'Percentage of query repeated',
    `total_ua_count`                    bigint  COMMENT 'Total distinct user-agents (from UA string)',
    `total_time`                        bigint  COMMENT 'Total time in milliseconds taken to run all the processed queries',
    `status_code_query_count`           map<bigint, bigint>  COMMENT 'Number of queries per status code',
    `query_time_class_query_count`      map<string, bigint>  COMMENT 'Number of queries per query time class'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''