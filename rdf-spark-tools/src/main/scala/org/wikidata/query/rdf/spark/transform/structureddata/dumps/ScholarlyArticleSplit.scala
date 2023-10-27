package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.SparkSession
import org.wikidata.query.rdf.spark.utils.SparkUtils
import scopt.OptionParser

/**
 * This job creates a partitioned table with scholarly article-related
 * triples from Wikidata in one partition and everything else in another.
 * There is technically some level of duplication between the partitions,
 * because there are common Value and Reference types associated with both.
 *
 * The job depends upon WikibaseRDFDumpConverter having successfully
 * imported Wikidata triples.
 *
 * Command line example:
 * spark3-submit \
 * --master yarn \
 * --driver-memory 16G \
 * --executor-memory 12G \
 * --executor-cores 4 \
 * --conf spark.driver.cores=2 \
 * --conf spark.executor.memoryOverhead=4g \
 * --conf spark.sql.shuffle.partitions=512 \
 * --conf spark.dynamicAllocation.maxExecutors=128 \
 * --conf spark.sql.autoBroadcastJoinThreshold=-1 \
 * --conf spark.yarn.maxAppAttempts=1 \
 * --class org.wikidata.query.rdf.spark.transform.structureddata.dumps.ScholarlyArticleSplit \
 * --name scholarly-article-spark \
 * ~yourusername/rdf-spark-tools-0.3.137-SNAPSHOT-jar-with-dependencies.jar \
 * --input-table-partition-spec discovery.wikibase_rdf/date=20231106/wiki=wikidata
 * --output-table-partition-spec yourusername.wikibase_rdf_scholarly_split/snapshot=20231106/wiki=wikidata
 */
case class ScholarlyArticleSplitParams(inputPartition: String = "", outputPartitionParent: String = "")

object ScholarlyArticleSplit {

  implicit val sparkSession: SparkSession = {
    SparkUtils.getSparkSession("ScholarlyArticleSplitWorker")
  }

  /**
   * Main method, parsing args and launching the partitioning
   *
   * @param args the arguments to parse
   */
  def main(args: Array[String]): Unit = {
    parseParams(args) match {
      case Some(params) => split(params)
      case _ => sys.exit(-1)
    }
  }

  def split(params: ScholarlyArticleSplitParams): Unit = {
    ScholarlyArticleSplitter.splitIntoPartitions(params)
  }

  /**
   * CLI Option Parser for job parameters (fill-in Params case class)
   */
  private def argsParser: OptionParser[ScholarlyArticleSplitParams] = {
    new OptionParser[ScholarlyArticleSplitParams]("") {
      head("Wikidata Scholarly Article Split", "")
      help("help") text "Prints this usage text"

      opt[String]('i', "input-table-partition-spec") required() valueName "<input-table-partition-spec>" action { (x, p) =>
        p.copy(inputPartition = x)
      } text "Input partition as source_database_name.source_table_name/date=YYYYMMDD/wiki=wikidata"

      opt[String]('o', "output-table-partition-spec") required() valueName "<output-table-partition-spec>" action { (x, p) =>
        p.copy(outputPartitionParent = x)
      } text "Output partition parent as target_database_name.target_table_name/snapshot=YYYYMMDD/wiki=wikidata"
    }
  }

  def parseParams(args: Array[String]): Option[ScholarlyArticleSplitParams] =
    argsParser.parse(args, ScholarlyArticleSplitParams()) match {
    case Some(params) =>
      if (params.inputPartition.isEmpty) {
        Console.err.print("--input-table-partition-spec must be provided\n")
        None
      }
      if (params.outputPartitionParent.isEmpty) {
        Console.err.print("--output-table-partition-spec must be provided\n")
        None
      }
      Some(params)
    case _ => None
  }
}
