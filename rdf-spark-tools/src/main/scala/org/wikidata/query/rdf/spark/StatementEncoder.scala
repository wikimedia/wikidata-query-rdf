package org.wikidata.query.rdf.spark

import javax.annotation.concurrent.NotThreadSafe

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.openrdf.model.{Statement, Value}
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.rio.ntriples.NTriplesUtil

/**
 * Encode statement Resource and Value using N3 format:
 * URI are wrapped inside <>
 * Strings are wrapped inside ""
 * Localized strings are wrapped in "" and suffixed with @lang
 * Data is wrapped inside "" and suffixed with type info ^^TypeURI
 */
@NotThreadSafe
class StatementEncoder extends Serializable {
  // on large graph (10B triples for wikidata as of march 2020)
  // we convert 4 values per triple.
  // Keeping a string buffer here we decrease StringBuffer allocations from
  // 40B (one per value) to 80M (one per entity)
  // drawback is that this class becomes not thread safe
  private val stringBuffer = new StringBuffer()

  private val valueFactory = new ValueFactoryImpl()

  def encode(st: Statement): Row = {
    if (st.getContext == null) {
      throw new IllegalArgumentException(s"Invalid context provided from triple: $st")
    }
    Row.fromTuple((
      encode(st.getContext),
      encode(st.getSubject),
      encode(st.getPredicate),
      encode(st.getObject)
    ): (String, String, String, String))
  }

  def decode(row: Row): Statement = {
    valueFactory.createStatement(
      NTriplesUtil.parseResource(row.getString(StatementEncoder.schema.fieldIndex("subject")), valueFactory),
      NTriplesUtil.parseURI(row.getString(StatementEncoder.schema.fieldIndex("predicate")), valueFactory),
      NTriplesUtil.parseValue(row.getString(StatementEncoder.schema.fieldIndex("object")), valueFactory),
      NTriplesUtil.parseResource(row.getString(StatementEncoder.schema.fieldIndex("context")), valueFactory))
  }

  def decode(value: String): Value = {
    NTriplesUtil.parseValue(value, valueFactory)
  }

  def encode(elt: Value): String = {
    stringBuffer.setLength(0)
    NTriplesUtil.append(elt, stringBuffer)
    stringBuffer.toString
  }

  def encodeURI(uri: String): String = {
    encode(valueFactory.createURI(uri))
  }
}

object StatementEncoder {
  val schema: StructType = StructType(Seq(
    StructField("context", StringType, nullable = false),
    StructField("subject", StringType, nullable = false),
    StructField("predicate", StringType, nullable = false),
    StructField("object", StringType, nullable = false)
  ))
}

