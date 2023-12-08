# A simple notebook demonstrating how to record query results and compare them

```python
import wmfdata
# Get the rdf-spark-tools artifact (spark-free assembly) somewhere in hdfs:
artifact_path = "hdfs:///user/dcausse/rdf-spark-tools-0.3.138-SNAPSHOT-spark-free.jar"

# Or get a totally customizable SparkSession using get_custom_session.
spark = wmfdata.spark.create_custom_session(
    master='yarn',
    spark_config={
        # 16g because we have many partition
        "spark.jars": artifact_path,
        "spark.driver.memory": "16g",
        "spark.driver.cores": 1,
        "spark.executor.memory": "8g",
        # XXX: Always set these values to something low: 3 concurrent queries at most!
        "spark.executor.cores": 1,
        "spark.dynamicAllocation.maxExecutors": 2,
        #"spark.executor.memoryOverhead": "1g",
        "spark.sql.shuffle.partitions": 10,
        'spark.locality.wait': '1s' # test 0
    }
)
spark.sparkContext.setCheckpointDir("hdfs:///user/dcausse/spark_checkpoints/wdqs_query_results_analysis")
```


```python
# Prepare the UDF that will extract the query results by running it against a sparql endpoint.
def query_recorder(endpoint, ua_suffix, spark):
    qr = spark.sparkContext._jvm.org.wikidata.query.rdf.spark.metrics.queries.QueryResultRecorder.create(endpoint, ua_suffix)
    from pyspark.sql.column import Column, _to_java_column
    def run(col):
        return Column(qr.apply(_to_java_column(col)))
    return run

# Declare two different endpoints (actually pointing at the same for testing purposes)
run_wdqs_sparql_endpoint_A = query_recorder("https://query.wikidata.org/sparql", "WDQS Split Analysis T352538", spark)
run_wdqs_sparql_endpoint_B = query_recorder("https://query.wikidata.org/sparql", "WDQS Split Analysis T352538", spark)
```


```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, Row

# An example input format containing an id, a query and a provenance (group)
inputType = StructType([StructField(name = "id", dataType = StringType()),
                        StructField(name = "query", dataType = StringType()),
                        StructField(name = "prov", dataType = StringType())])
inputDf = spark.createDataFrame([
    Row("id1", "SELECT * { ?s ?p ?o . } LIMIT 1", "provenance 1"),
    Row("id2", "SELECT * { ?s wdt:P31 wd:Q5 . } LIMIT 5", "provenance 1"),
    Row("id3", "SELECT * { ?s wdt:P31 wd:Q18918145 . } LIMIT 10", "provenance 2"),
    Row("id4", "SELECT * { ?s wdt:P31 wd:Q523 . } LIMIT 12", "provenance 2")
], inputType)
```


```python
from pyspark.sql.functions import *

# Run the costly extraction
# Be sure to checkpoint() the output or save this to a dedicate table so that
# you don't recompute the results from the wdqs endpoints

# This is really important to make sure that your spark session won't attempt
# to parallelize aggressively this process, check your spark configs first!
# If unsure please ask before running it.

results_A = (inputDf
           .withColumn("results", run_wdqs_sparql_endpoint_A(inputDf["query"]))
           .select("results.*", inputDf["*"])
           .withColumn("endpoint", lit("A"))
           .alias("A")
           .checkpoint())

results_B = (inputDf
           .withColumn("results", run_wdqs_sparql_endpoint_B(inputDf["query"]))
           .select("results.*", inputDf["*"])
           .withColumn("endpoint", lit("B"))
           .alias("B")
           .checkpoint())
```


```python
results_A.printSchema()
```

    root
     |-- results: array (nullable = true)
     |    |-- element: map (containsNull = true)
     |    |    |-- key: string
     |    |    |-- value: string (valueContainsNull = true)
     |-- success: boolean (nullable = true)
     |-- error_msg: string (nullable = true)
     |-- resultSize: integer (nullable = true)
     |-- exactHash: string (nullable = true)
     |-- reorderedHash: string (nullable = true)
     |-- id: string (nullable = true)
     |-- query: string (nullable = true)
     |-- prov: string (nullable = true)
     |-- endpoint: string (nullable = false)



# Extract some comparison metrics out of two dataframes

* total: total number of queries in the group
* both_success: total number of queries that are successful in both groups
* both_failed: total number of queries that failed in both groups
* same: total number of queries that have identical results (when both successful)
* same_unordered: total number of queries that have identical results when re-ordered (when both successful)
* same_result_size: total number of queries that have identical results when re-ordered (when both successful)
* [AB]_total_success: total number of successful queries in A and B
* [AB]_total_result_size: total number of results in group A and B
* [AB]_avg_result_size: avg of the result size in group A and B


```python
def aggA_B(fun, sourceCol, destCol):
    return [
      fun(col("A." + sourceCol)).alias("A_" + destCol),
      fun(col("B." + sourceCol)).alias("B_" + destCol)
    ]

comparison = (results_A.alias("A")
      .join(results_B.alias("B"), (results_A["id"] == results_B["id"]) & (results_A["prov"] == results_B["prov"]))
      .withColumn("both_success", (col("A.success") == col("B.success")) & (col("A.success") == lit(True)))
      .withColumn("both_failed", (col("A.success") == col("B.success")) & (col("A.success") == lit(False)))
      .withColumn("same", (col("both_success") == lit(True)) & (col("A.exactHash") == col("B.exactHash")))
      .withColumn("same_reordered", (col("both_success") == lit(True)) & (col("A.reorderedHash") == col("B.reorderedHash")))
      .withColumn("same_result_size", (col("both_success") == lit(True)) & (col("A.resultSize") == col("B.resultSize")))
      .groupBy(results_A["prov"])
      .agg(
        *[sum(lit(1)).alias("total"),
          sum(col("both_success").cast(LongType())).alias("total_both_success"),
          sum(col("both_failed").cast(LongType())).alias("total_both_failed"),
          sum(col("same").cast(LongType())).alias("total_same"),
          sum(col("same_reordered").cast(LongType())).alias("total_same_reordered"),
          sum(col("same_result_size").cast(LongType())).alias("same_result_size"),
          *aggA_B(lambda c: sum(c.cast(LongType())), "success", "total_success"),
          *aggA_B(lambda c: sum(c.cast(LongType())), "resultSize", "total_result_size"),
          *aggA_B(lambda c: avg(c.cast(LongType())), "resultSize", "avg_result_size"),
         ])
      )

```


```python
comparison.toPandas()
```






<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>prov</th>
      <th>total</th>
      <th>total_both_success</th>
      <th>total_both_failed</th>
      <th>total_same</th>
      <th>total_same_reordered</th>
      <th>same_result_size</th>
      <th>A_total_success</th>
      <th>B_total_success</th>
      <th>A_total_result_size</th>
      <th>B_total_result_size</th>
      <th>A_avg_result_size</th>
      <th>B_avg_result_size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>provenance 2</td>
      <td>2</td>
      <td>2</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
      <td>2</td>
      <td>2</td>
      <td>2</td>
      <td>22</td>
      <td>22</td>
      <td>11.0</td>
      <td>11.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>provenance 1</td>
      <td>2</td>
      <td>2</td>
      <td>0</td>
      <td>2</td>
      <td>2</td>
      <td>2</td>
      <td>2</td>
      <td>2</td>
      <td>6</td>
      <td>6</td>
      <td>3.0</td>
      <td>3.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
results_A.toPandas()
```






<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>results</th>
      <th>success</th>
      <th>error_msg</th>
      <th>resultSize</th>
      <th>exactHash</th>
      <th>reorderedHash</th>
      <th>id</th>
      <th>query</th>
      <th>prov</th>
      <th>endpoint</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>[{'p': '&lt;http://creativecommons.org/ns#license...</td>
      <td>True</td>
      <td>None</td>
      <td>1</td>
      <td>61875a897762715050915a296e4e42afbae578465a5929...</td>
      <td>6c343dc4ace5220b59ed9d7856f5b7a13cd17538438847...</td>
      <td>id1</td>
      <td>SELECT * { ?s ?p ?o . } LIMIT 1</td>
      <td>provenance 1</td>
      <td>A</td>
    </tr>
    <tr>
      <th>1</th>
      <td>[{'s': '&lt;http://www.wikidata.org/entity/L12157...</td>
      <td>True</td>
      <td>None</td>
      <td>5</td>
      <td>dcc119d3ab40eb1f82c721c69f978056dfdb9e0e366622...</td>
      <td>a59a0668822ed7183887225605f2c4b545b9e3210600f1...</td>
      <td>id2</td>
      <td>SELECT * { ?s wdt:P31 wd:Q5 . } LIMIT 5</td>
      <td>provenance 1</td>
      <td>A</td>
    </tr>
    <tr>
      <th>2</th>
      <td>[{'s': '&lt;http://www.wikidata.org/entity/Q26769...</td>
      <td>True</td>
      <td>None</td>
      <td>10</td>
      <td>0257fa4e9184512af6042a84fe2dbffd653a0222a6d66e...</td>
      <td>57411c9841a92402009fd2a28089b8556c65915612618c...</td>
      <td>id3</td>
      <td>SELECT * { ?s wdt:P31 wd:Q18918145 . } LIMIT 10</td>
      <td>provenance 2</td>
      <td>A</td>
    </tr>
    <tr>
      <th>3</th>
      <td>[{'s': '&lt;http://www.wikidata.org/entity/Q4875&gt;...</td>
      <td>True</td>
      <td>None</td>
      <td>12</td>
      <td>644b8fd30421c242a261e4e9eb83cce05fb8481f31bb45...</td>
      <td>78fb095804f80fb4f9161b9f5be4610069c0673ffdae06...</td>
      <td>id4</td>
      <td>SELECT * { ?s wdt:P31 wd:Q523 . } LIMIT 12</td>
      <td>provenance 2</td>
      <td>A</td>
    </tr>
  </tbody>
</table>
</div>
