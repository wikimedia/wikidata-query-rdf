package org.wikidata.query.rdf.spark.metrics.queries

import com.google.common.hash.{HashFunction, Hashing}
import com.ibm.icu.text.{CollationKey, Collator}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, functions}
import org.eclipse.jetty.http.{HttpField, HttpHeader}
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.model.{BNode, Value, ValueFactory}
import org.openrdf.query.impl.{ListBindingSet, TupleQueryResultImpl}
import org.openrdf.query.{Binding, BindingSet, TupleQueryResult}
import org.openrdf.rio.ntriples.NTriplesUtil
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.tool.rdf.client.RdfClient

import java.io.Serializable
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Locale
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * QueryResultRecorder is a class usable as a spark UDF to run a SPARQL query against and record its output.
 *
 * The class can be used directly to obtain the QueryResult via the query(sparl String) method.
 * Or via a udf using its own apply function:
 * {{{
 *   val recorder = QueryResultRecorder.create("https://query.wikidata.org/sparql");
 *   val df = ...; // a dataframe with a string column named "query"
 *   val results = df.select(recorder(df("query")).alias("query_result"))
 * }}}
 * The dataframe results will have column name "query_result" of type struct with the schema [[QueryResultRecorder.outputStruct]]:
 * <ul>
 *   <li>results: list of maps of var names to N3 value representation of the results</li>
 *   <li>success: a boolean indicating if the query succeeded or not</li>
 *   <li>error_msg: a message with the error if the query failed</li>
 *   <li>exactHash: a SHA256 of the results</li>
 *   <li>reorderedHash: a SHA256 of the reordered results</li>
 *   <li>resultSize: the size in number of solutions of the result</li>
 * </ul>
 *
 * NOTE: extra care must be taken when executing this with spark to not overload a SPARQL endpoint.
 *
 * @param rdfClientBuilder the RDFClient supplier
 */

@SerialVersionUID(1)
class QueryResultRecorder(rdfClientBuilder: () => RdfClient) extends Serializable {

  @transient
  private lazy val hashFunction: HashFunction = Hashing.sha256()
  @transient
  private lazy val client: RdfClient = rdfClientBuilder()
  @transient
  private lazy val valueFactory: ValueFactory = new ValueFactoryImpl()
  @transient
  private lazy val udf: UserDefinedFunction = {
    def udfFunc: UDF1[String,Row] = (query: String) => {
      this.query(query) match {
        case QueryResult(Some(results), true, msg, Some(size), Some(exactHash), Some(reorderedHash)) =>
          Row(results, true, msg.orNull, size, exactHash, reorderedHash)
        case QueryResult(results, false, Some(msg), size, exactHash, reorderedHash) =>
          Row(results.orNull, false, msg, size.orNull, exactHash.orNull, reorderedHash.orNull)
        case x: QueryResult => throw new IllegalArgumentException("Invalid QueryResult for query " + query + ": " + x)
      }
    }
    functions.udf(udfFunc, QueryResultRecorder.outputStruct)
  }

  private lazy val collator: Collator = {
    val collator = Collator.getInstance(Locale.ROOT)
    // Set tertiary strength (same as blazegraph, for context see T233204)
    collator.setStrength(Collator.TERTIARY)
    collator
  }
  def apply(column: Column): Column = {
    udf(column)
  }

  private def fromAsk(result: Boolean): QueryResult = {
    val names = List("b").asJava
    fromResult(new TupleQueryResultImpl(names, List(new ListBindingSet(names, valueFactory.createLiteral(result))).asJava))
  }

  def query(query: String): QueryResult = {
    def execute(query: String): QueryResult = {
      QueryResultRecorder.getQueryType(query) match {
        case "SELECT" => fromResult(client.query(query))
        case "ASK" => fromAsk(client.ask(query))
        case "DESCRIBE" => fromResult(client.describe(query))
        case "CONSTRUCT" => fromResult(client.construct(query))
      }
    }

    Try(execute(query)) match {
      case Success(result) => result
      case Failure(e) =>
        QueryResult(results = None, success = false,
          Some(e.getMessage + ": " + ExceptionUtils.getStackTrace(e)), resultSize = None, exactHash = None, reorderedHash = None)
    }
  }


  private def fromResult(result: TupleQueryResult) = {
    val (bindings, solutions) = copyResult(result)
    val sortedBindings = bindings.sorted
    QueryResult(
      results = Some(QueryResultRecorder.encodeQueryResults(solutions)),
      success = true,
      errorMessage = None,
      resultSize = Some(solutions.length),
      exactHash = Some(hashSolutions(bindings, solutions)),
      reorderedHash = Some(hashSolutions(sortedBindings, solutions.sorted(sortBindingSet(sortedBindings))))
    )
  }


  private def sortBindingSet(bindings: Array[String]): Ordering[BindingSet] = {
    (x: BindingSet, y: BindingSet) => {
      bindings map {
        b => (Option(x.getBinding(b)), Option(y.getBinding(b)))
      } map {
        case (None, None) => 0
        case (Some(_), None) => 1 // left|null > null
        case (None, Some(_)) => -1 // null < right
        case (Some(bvx), Some(bvy)) => // actual comparison
          compareBinding(bvx, bvy)
      } collectFirst {
        case cmp: Int if cmp != 0 => cmp
      } getOrElse 0
    }
  }

  private def compareBinding(bvx: Binding, bvy: Binding) = {
    (bvx.getValue, bvy.getValue) match {
      // consider two blank nodes as equal, they're assigned by blazegraph while generating the results
      // if we re-order there's no point in keeping the specific blank node id.
      // Risk of missed positives seems low enough compared to the noise caused by false positives.
      case (_: BNode, _: BNode) => 0
      case (_: BNode, _: Value) => -1
      case (_: Value, _: BNode) => 1
      case (vx: Value, vy: Value) =>
        vx.getClass.getName.compareTo(vy.getClass.getName) match {
          case 0 =>
            encodeValue(vx).compareTo(encodeValue(vy))
          case x: Int => x
        }
    }
  }

  private def hashSolutions(bindings: Array[String], solutions: Iterable[BindingSet]): String = {
    val hasher = hashFunction.newHasher()
    bindings foreach {
      hasher.putString(_, StandardCharsets.UTF_8)
    }
    solutions foreach { bs =>
      bindings map bs.getBinding map (Option(_)) foreach {
        case Some(b) =>
          b.getValue match {
            case _: BNode => hasher.putString("_BLANK")
            case v: Value =>
              hasher.putString(v.getClass.getName, StandardCharsets.UTF_8)
              hasher.putBytes(encodeValue(v).toByteArray)
          }
        case None =>
          hasher.putInt(-1)
      }
    }
    hasher.hash().toString
  }

  private def encodeValue(v: Value): CollationKey = {
    collator.getCollationKey(v.toString)
  }

  private def copyResult(result: TupleQueryResult): (Array[String], Array[BindingSet]) = {
    val bindings = result.getBindingNames.toArray(Array[String]())
    val solutions: ListBuffer[BindingSet] = ListBuffer()
    while (result.hasNext) {
      solutions += result.next()
    }
    (bindings, solutions.toArray)
  }

}

@SerialVersionUID(1)
object QueryResultRecorder {
  private val IRI_PATTERN = Pattern.compile("^<([^>]*)>*")

  private val PREFIX_PATTERN = Pattern.compile("^prefix([^:]+):", Pattern.CASE_INSENSITIVE)

  private val COMMENT_PATTERN = Pattern.compile("^(#.*((\r)?\n|(\r)?\n*))*")
  private val QUERY_TYPE = Pattern.compile("^(SELECT|ASK|CONSTRUCT|DESCRIBE)", Pattern.CASE_INSENSITIVE)

  def create(endpoint: String, uaSuffix: String): QueryResultRecorder = {
    new QueryResultRecorder(() => {
      val timeout = Duration.ofSeconds(65)
      val httpClient = HttpClientUtils.buildHttpClient(None.orNull, None.orNull)
      httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, "QueryResultRecorder (org.wikidata.query.rdf:rdf-spark-tools) bot " + uaSuffix))
      new RdfClient(httpClient, URI.create(endpoint), HttpClientUtils.buildHttpClientRetryer(), timeout, 16*1024*1024)
    })
  }

  def encodeQueryResults(solutions: Array[BindingSet]): Array[Map[String, String]] = {
    solutions map { s =>
      s.asScala map { b => b.getName -> NTriplesUtil.toNTriplesString(b.getValue) } toMap
    }
  }

  private val resultList: ArrayType = ArrayType(MapType(DataTypes.StringType, DataTypes.StringType, valueContainsNull = true))

  /**
   * The schema of the output of the UDF
   */
  val outputStruct: StructType = new StructType(Array(
    StructField(name = "results", dataType = resultList),
    StructField(name = "success", dataType = DataTypes.BooleanType),
    StructField(name = "error_msg", dataType = DataTypes.StringType),
    StructField(name = "resultSize", dataType = DataTypes.IntegerType),
    StructField(name = "exactHash", dataType = DataTypes.StringType),
    StructField(name = "reorderedHash", dataType = DataTypes.StringType)
  ))

  // scalastyle:off cyclomatic.complexity
  def getQueryType(input: CharSequence): CharSequence  = {
    var i = 0
    var restOfQuery: CharSequence = ""
    while (i < input.length) {
      val c = input.charAt(i)
      c match {
        case '#' =>
          i += readComment(input, i)

        case 'p' | 'P' =>
          // read PREFIX
          i += readPrefix(input, i)

        case 'b' | 'B' =>
          i += 4 // 4 for base keyword

        case '<' =>
          // read IRI
          i += readIRI(input, i)

        case _ =>
          if (Character.isWhitespace(c)) {
            i += 1
          } else {
            restOfQuery = input.subSequence(i, input.length)
            i += restOfQuery.length
          }

      }
    }

    val m = QUERY_TYPE.matcher(restOfQuery)
    if (m.find()) {
      m.group().toUpperCase(Locale.ROOT)
    } else {
      "SELECT"
    }
  }
  // scalastyle:on cyclomatic.complexity

  /**
   * Reads the first comment line from the input, and returns
   * the comment line (including the line break character) without
   * the leading "#".
   */
  private def readComment(input: CharSequence, index: Int): Int = {
    val matcher = COMMENT_PATTERN.matcher(input.subSequence(index, input.length()))
    if (matcher.find()) {
      matcher.end()
    } else {
      1
    }
  }

  private def readPrefix(input: CharSequence, index: Int): Int = {
    val matcher = PREFIX_PATTERN.matcher(input.subSequence(index, input.length()))
    if (matcher.find()) {
      matcher.end()
    } else {
      1
    }
  }

  private def readIRI(input: CharSequence, index: Int): Int = {
    val matcher = IRI_PATTERN.matcher(input.subSequence(index, input.length()))
    if (matcher.find()) {
      matcher.end()
    } else {
      1
    }
  }

}

case class QueryResult(results: Option[Array[Map[String, String]]],
                       success: Boolean,
                       errorMessage: Option[String],
                       resultSize: Option[Int],
                       exactHash: Option[String],
                       reorderedHash: Option[String]
                      )
