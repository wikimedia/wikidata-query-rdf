package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit}
import org.wikidata.query.rdf.spark.utils.SparkUtils
import scopt.OptionParser

/**
 * This job creates a set of gzip encoded simple .ttl files from
 * a table with string columns for "subject", "predicate", "object",
 * and the "context" qualifier (i.e., quads).
 *
 * Currently, the job is intended for ScholarlyArticleSplit having
 * generated tables, which are standard Parquet based tables and
 * that are based on well-formed "subject", "predicate", and "object"
 * and "context" strings already. But the job could be used on similar
 * tables from other jobs.
 *
 * The output is simply concatenated strings one line at a time like so.
 *
 * <http://www.wikidata.org/entity/Q121824936> <http://www.wikidata.org/prop/direct/P433> "02" .
 *
 * The files will take on names in HDFS such as:
 *  part-00000-5bfcba5b-4fa7-4716-9d0d-55bd030f1e60-c000.ttl.gz
 *  part-00001-5bfcba5b-4fa7-4716-9d0d-55bd030f1e60-c000.ttl.gz
 *  ...
 *  part-01023-5bfcba5b-4fa7-4716-9d0d-55bd030f1e60-c000.ttl.gz
 *
 * If one wants to use the loadData.sh script there is a shortcut option.
 * One can modify loadData.sh so that its filename matching pattern reflects
 * that of the produced files. Note that these filenames start with a 00000
 * sequence number, but the script was built with the assumption of looping
 * from a 1-based system. The following should get the job done.
 * ./loadData.sh -n wdq -d /media/path/to/gzips -s 0 -e 0
 * ./loadData.sh -n wdq -d /media/path/to/gzips -s 1 -e 1023
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
 * --class org.wikidata.query.rdf.spark.transform.structureddata.dumps.NTripleGenerator \
 * --name wikibase-rdf-statements-spark \
 * ~yourusername/rdf-spark-tools-0.3.138-SNAPSHOT-jar-with-dependencies.jar \
 * --input-table-partition-spec discovery.wikibase_rdf_scholarly_split/snapshot=20231006/wiki=wikidata/scope=wikidata_main \
 * --output-hdfs-path hdfs://fully/qualified/hdfs/path \
 * --num-partitions 1024
 */

case class NTripleGeneratorParams(inputPartition: String = "", outputPath: String = "", numPartitions: Int = 1024)

object NTripleGenerator {

  implicit val sparkSession: SparkSession = {
    SparkUtils.getSparkSession("NTripleGeneratorWorker")
  }

  /**
   * Main method, parsing args and launching the generator
   *
   * @param args the arguments to parse
   */
  def main(args: Array[String]): Unit = {
    parseParams(args) match {
      case Some(params) =>
        generateNTriples(params.inputPartition, params.outputPath, params.numPartitions)
      case _ => sys.exit(-1)
    }
  }

  def generateNTriples(
                       inputTablePartitionSpec: String,
                       outputHdfsPath: String,
                       numPartitions: Int
                      )(implicit spark: SparkSession): Unit = {
    SparkUtils.readTablePartition(inputTablePartitionSpec)(spark)
      .repartitionByRange(numPartitions, col("context"), col("subject"))
      .sortWithinPartitions("context", "subject")
      .select(
        concat(
          col("subject"),
          lit(" "),
          col("predicate"),
          lit(" "),
          col("object"),
          lit(" .")))
      .write
      .option("compression", "gzip")
      .text(outputHdfsPath)
  }

  /**
   * CLI Option Parser for job parameters (fill-in Params case class)
   */
  private def argsParser: OptionParser[NTripleGeneratorParams] = {
    new OptionParser[NTripleGeneratorParams]("") {
      head("Wikidata N-Triple Generator", "")
      help("help") text "Prints this usage text"

      opt[String]('i', "input-table-partition-spec") required() valueName "<input-table-partition-spec>" action { (x, p) =>
        p.copy(inputPartition = x)
      } text "Input partition as source_database_name.source_table_name/date=YYYYMMDD/wiki=wikidata/scope=<scope>"

      opt[String]('o', "output-hdfs-path") required() valueName "<output-hdfs-path>" action { (x, p) =>
        p.copy(outputPath = x)
      } text "Output fully qualified HDFS destination path as hdfs://path/to/output."

      opt[Int]('n', "num-partitions") optional() action { (x, p) =>
        p.copy(numPartitions = x)
      } text "Number of partitions to use (output files in HDFS). Defaults to 1024."
    }
  }

  def parseParams(args: Array[String]): Option[NTripleGeneratorParams] =
    argsParser.parse(args, NTripleGeneratorParams()) match {
    case Some(params) =>
      if (params.inputPartition.isEmpty) {
        Console.err.print("--input-table-partition-spec must be provided\n")
        None
      }
      if (params.outputPath.isEmpty) {
        Console.err.print("--output-hdfs-path must be provided\n")
        None
      }
      Some(params)
    case _ => None
  }
}
