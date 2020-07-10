package org.wikidata.query.rdf.spark

import org.apache.spark.sql.Row
import org.openrdf.model.impl.ValueFactoryImpl
import org.scalatest.{FlatSpec, Matchers}

class StatementEncoderUnitTest extends FlatSpec with Matchers {
  val statementEncoder = new StatementEncoder()
  val valueFactory = new ValueFactoryImpl()
  "a statement" should "be encodable as spark Row" in {
    val st = valueFactory.createStatement(
      valueFactory.createURI("http://foo.local/something"),
      valueFactory.createURI("http://foo.local/link"),
      valueFactory.createURI("http://foo.../what"),
      valueFactory.createURI("http://foo.local/bar")
    )
    statementEncoder.encode(st) should equal((
      "<http://foo.local/bar>",
      "<http://foo.local/something>",
      "<http://foo.local/link>",
      "<http://foo.../what>"
    ))
  }

  "a statement" should "be decodable from spark Row" in {
    val st = valueFactory.createStatement(
      valueFactory.createURI("http://foo.local/something"),
      valueFactory.createURI("http://foo.local/link"),
      valueFactory.createURI("http://foo.../what"),
      valueFactory.createURI("http://foo.local/bar")
    )
    statementEncoder.decode(Row.fromTuple(
      "<http://foo.local/bar>",
      "<http://foo.local/something>",
      "<http://foo.local/link>",
      "<http://foo.../what>"
    )) should equal(st)
  }

  "a string" should "be encoded" in {
    statementEncoder.encode(valueFactory.createLiteral("value")) should equal(""""value"""")
  }

  "a string" should "be decoded" in {
    valueFactory.createLiteral("value") should equal(statementEncoder.decode(""""value""""))
  }

  "a localized string" should "be encoded" in {
    statementEncoder.encode(valueFactory.createLiteral("value", "fr")) should equal(""""value"@fr""")
  }

  "a localized string" should "be decoded" in {
    valueFactory.createLiteral("value", "fr") should equal(statementEncoder.decode(""""value"@fr"""))
  }

  "a number" should "be encoded" in {
    statementEncoder.encode(valueFactory.createLiteral(123)) should equal(""""123"^^<http://www.w3.org/2001/XMLSchema#int>""")
  }

  "a number string" should "be decoded" in {
    valueFactory.createLiteral(123) should equal(statementEncoder.decode(""""123"^^<http://www.w3.org/2001/XMLSchema#int>"""))
  }

  "a URI" should "be directly encoded" in {
    statementEncoder.encodeURI("http://test.local") should equal("<http://test.local>")
  }

}
