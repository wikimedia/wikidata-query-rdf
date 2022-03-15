CREATE TABLE IF NOT EXISTS `per_subgraph_query_metrics` (
    `subgraph`                             string  COMMENT 'URI of the subgraphs in wikidata',
    `query_count`                          bigint  COMMENT 'Number of queries accessing this subgraph',
    `query_time`                           bigint  COMMENT 'Total time of queries accessing this subgraph',
    `ua_count`                             bigint  COMMENT 'Number of distinct user agents accessing this subgraph',
    `query_type`                           bigint  COMMENT 'Distinct types of queries in this subgraph (rough estimate from operator list)',
    `percent_query_count`                  double  COMMENT 'Percent queries in this subgraph w.r.t total parsed queries',
    `percent_query_time`                   double  COMMENT 'Percent total query time in this subgraph w.r.t total parsed query time',
    `percent_ua_count`                     double  COMMENT 'Percent unique user-agents w.r.t total user-agents of parsed queries',
    `query_count_rank`                     integer COMMENT 'Rank of the subgraph in terms of number of query in descending order',
    `query_time_rank`                      integer COMMENT 'Rank of the subgraph in terms of query time in descending order',
    `avg_query_time`                       double  COMMENT 'Average time(ms) per query in this subgraph',
    `qid_count`                            bigint  COMMENT 'Number of queries that matched to this subgraph due to the subgraph Qid match',
    `item_count`                           bigint  COMMENT 'Number of queries that matched to this subgraph due to the subgraph items Qid match',
    `pred_count`                           bigint  COMMENT 'Number of queries that matched to this subgraph due to predicate match',
    `uri_count`                            bigint  COMMENT 'Number of queries that matched to this subgraph due to URI match',
    `literal_count`                        bigint  COMMENT 'Number of queries that matched to this subgraph due to literals match',
    `query_time_class_counts`              map<string, bigint>  COMMENT 'Number of queries per query time class',
    `ua_info`                              array<
                                               struct<
                                                    ua_rank: integer,
                                                    ua_query_count: bigint,
                                                    ua_query_time: bigint,
                                                    ua_query_type: bigint,
                                                    ua_query_percent: double,
                                                    ua_query_time_percent: double,
                                                    ua_avg_query_time: double,
                                                    ua_query_type_percent: double
                                                >
                                            >      COMMENT 'List of top user-agents (by query count) using this subgraph and other aggregate info about its queries. The percents are w.r.t the subgraphs data',
    `ua_query_count_percentiles`           array<double>  COMMENT 'List of 0.1 to 0.9 percentile of query count per user-agent in each subgraph',
    `ua_query_count_mean`                  double  COMMENT 'Mean of query count per user-agent in each subgraph',
    `subgraph_composition`                 array<
                                                struct<
                                                    item: boolean,
                                                    predicate: boolean,
                                                    uri: boolean,
                                                    qid: boolean,
                                                    literal: boolean,
                                                    count: bigint
                                                    >
                                                >  COMMENT 'List of various combinations is which queries match with a subgraph and the number of such queries',
    `query_only_accessing_this_subgraph`   bigint  COMMENT 'Number of queries that access only this subgraph alone',
    `top_items`                            map<string, bigint>  COMMENT 'Top items in this subgraph that caused a query match mapped to the number of queries that matched',
    `matched_items_percentiles`            array<double>  COMMENT 'List of 0.1 to 0.9 percentile of queries matched per item in each subgraph',
    `matched_items_mean`                   double  COMMENT 'Mean of queries matched per item in this subgraph',
    `top_predicates`                       map<string, bigint>  COMMENT 'Top predicates in this subgraph that caused a query match mapped to the number of queries that matched',
    `matched_predicates_percentiles`       array<double>  COMMENT 'List of 0.1 to 0.9 percentile of queries matched per predicate in each subgraph',
    `matched_predicates_mean`              double  COMMENT 'Mean of queries matched per predicate in this subgraph',
    `top_uris`                             map<string, bigint>  COMMENT 'Top URIs in this subgraph that caused a query match mapped to the number of queries that matched',
    `matched_uris_percentiles`             array<double>  COMMENT 'List of 0.1 to 0.9 percentile of queries matched per URI in each subgraph',
    `matched_uris_mean`                    double  COMMENT 'Mean of queries matched per URI in this subgraph',
    `service_counts`                       map<string, bigint>  COMMENT 'List of services and the number of queries that use the service',
    `path_counts`                          map<string, bigint>  COMMENT 'List of top paths used and the number of queries that use the path'
)
PARTITIONED BY (
    `year`              int     COMMENT 'Unpadded year of queries',
    `month`             int     COMMENT 'Unpadded month of queries',
    `day`               int     COMMENT 'Unpadded day of queries',
    `wiki`              string  COMMENT 'Wiki name: one of {wikidata, commons}'
)
STORED AS PARQUET
LOCATION ''