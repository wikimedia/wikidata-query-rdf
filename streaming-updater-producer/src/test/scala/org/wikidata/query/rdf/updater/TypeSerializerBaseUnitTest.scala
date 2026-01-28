package org.wikidata.query.rdf.updater

import java.io.{ByteArrayInputStream, File}
import java.nio.file.Files
import java.time.Instant

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.core.memory.{DataInputView, DataInputViewStreamWrapper, DataOutputView, DataOutputViewStreamWrapper}
import org.apache.flink.util.InstantiationUtil
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}

class TypeSerializerBaseUnitTest extends FlatSpec with Matchers {
  private val writeFixtureToTmp = false
  "various components" should "be serializable" in {
    InstantiationUtil.isSerializable(new MyTypeInfo()) shouldBe true
    InstantiationUtil.isSerializable(new SerializerHelper(1)) shouldBe true
  }

  "TypeSerializerBase" should "infer type and returns proper metadata" in {
    val serializerBase = new TypeSerializerBase[String](1) {
      override def deserializeRecord()(implicit source: DataInputView): String = ???
      override def serializeRecord(record: String)(implicit output: DataOutputView): Unit = ???
      override def snapshotConfiguration(): TypeSerializerSnapshot[String] = ???
    }
    InstantiationUtil.isSerializable(serializerBase) shouldBe true
    serializerBase.isImmutableType shouldBe true
    serializerBase.duplicate() shouldBe serializerBase
    Option(serializerBase.createInstance()) shouldBe None
    serializerBase.copy("test") shouldBe "test"
    serializerBase.copy("test", "reused") shouldBe "test"
    serializerBase.getLength shouldBe -1
    serializerBase.readVersion shouldBe 1
  }

  "BaseTypeInfo" should "infer type and returns proper metadata" in {
    val typeInfo = new MyTypeInfo
    typeInfo.getTypeClass shouldEqual classOf[String]
    typeInfo.isKeyType shouldBe false
    typeInfo.isTupleType shouldBe false
    typeInfo.isBasicType shouldBe false
    typeInfo.isSortKeyType shouldBe false
    typeInfo.getArity shouldBe 1
    typeInfo.getTotalFields shouldBe 1
    typeInfo.toString shouldEqual "MyTypeInfo[String]"
  }

  "various components" should "be handle equals" in {
    val myTypeInfo = new MyTypeInfo()
    myTypeInfo == InstantiationUtil.clone(myTypeInfo) shouldBe true
    myTypeInfo.hashCode shouldEqual InstantiationUtil.clone(myTypeInfo).hashCode
    val serHelper = new SerializerHelper(1)
    serHelper shouldEqual InstantiationUtil.clone(serHelper)
    serHelper.hashCode shouldEqual InstantiationUtil.clone(serHelper).hashCode
  }

  "Serialization helper" should "serialize objects properly" in {
    val helper = new SerializerHelper(1)
    helper.readVersion shouldEqual 1

    val baos = new ByteArrayOutputStream()
    implicit val output: DataOutputView = new DataOutputViewStreamWrapper(baos)
    helper.writeInstant(Instant.ofEpochMilli(123))
    helper.writeOptionalInstant(Some(Instant.ofEpochMilli(234)))
    helper.writeOptionalInstant(None)
    helper.writeOptionalLong(Some(345))
    helper.writeOptionalLong(None)
    helper.writeOptionalString(Some("string"))
    helper.writeOptionalString(None)
    val revCreate = RevCreate("Q1", Instant.ofEpochMilli(456), 1, None, Instant.ofEpochMilli(567),
      new EventInfo(new EventsMeta(Instant.ofEpochMilli(678), "ID", "domain", "stream", "req_id"), "schema"))
    helper.writeBasicEventData(revCreate)
    baos.close()

    if (writeFixtureToTmp) {
      Files.write(new File("/tmp/SerializerHelper.v" + + helper.readVersion + ".bin").toPath, baos.toByteArray)
    }

    implicit val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(new ByteArrayInputStream(baos.toByteArray))
    helper.readInstant() shouldBe Instant.ofEpochMilli(123)
    helper.readOptionalInstant() shouldBe Some(Instant.ofEpochMilli(234))
    helper.readOptionalInstant() shouldBe None
    helper.readOptionalLong() shouldBe Some(345)
    helper.readOptionalLong() shouldBe None
    helper.readOptionalString() shouldBe Some("string")
    helper.readOptionalString() shouldBe None

    val (item, eventTime, ingestionTime, revision, eventInfo) = helper.readBasicEventData()(input)
    item shouldBe "Q1"
    eventTime shouldBe Instant.ofEpochMilli(456)
    ingestionTime shouldBe Instant.ofEpochMilli(567)
    revision shouldBe 1
    eventInfo shouldBe new EventInfo(new EventsMeta(Instant.ofEpochMilli(678), "ID", "domain", "stream", "req_id"), "schema")
  }

  "SerializationHelper V1 format" should "be readable" in {
    implicit val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(this.getClass.getResourceAsStream("SerializerHelper.v1.bin"))
    val helper = new SerializerHelper(1)
    helper.readInstant() shouldBe Instant.ofEpochMilli(123)
    helper.readOptionalInstant() shouldBe Some(Instant.ofEpochMilli(234))
    helper.readOptionalInstant() shouldBe None
    helper.readOptionalLong() shouldBe Some(345)
    helper.readOptionalLong() shouldBe None
    helper.readOptionalString() shouldBe Some("string")
    helper.readOptionalString() shouldBe None

    val (item, eventTime, ingestionTime, revision, eventInfo) = helper.readBasicEventData()(input)
    item shouldBe "Q1"
    eventTime shouldBe Instant.ofEpochMilli(456)
    ingestionTime shouldBe Instant.ofEpochMilli(567)
    revision shouldBe 1
    eventInfo shouldBe new EventInfo(new EventsMeta(Instant.ofEpochMilli(678), "ID", "domain", "stream", "req_id"), "schema")
  }

  "VersionedCustomSerializerSnapshot" should "be serializable and provide proper compatibility checks" in {
    val serializerFactory = (v: Int) => new MyTypeSerializer(v)
    val versionedCustomSerializerSnapshot = new VersionedCustomSerializerSnapshot(1, serializerFactory)
    versionedCustomSerializerSnapshot.getCurrentVersion shouldBe 1
    InstantiationUtil.isSerializable(versionedCustomSerializerSnapshot)

    val baos = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(baos)
    versionedCustomSerializerSnapshot.writeSnapshot(output)
    output.close()
    baos.toByteArray shouldBe empty
    versionedCustomSerializerSnapshot.readSnapshot(1, new DataInputViewStreamWrapper(new ByteArrayInputStream(baos.toByteArray)), this.getClass.getClassLoader)

    versionedCustomSerializerSnapshot.restoreSerializer() shouldEqual new MyTypeSerializer(1)

    versionedCustomSerializerSnapshot.resolveSchemaCompatibility(new VersionedCustomSerializerSnapshot(1, serializerFactory)).isCompatibleAsIs shouldBe true
    versionedCustomSerializerSnapshot.resolveSchemaCompatibility(new VersionedCustomSerializerSnapshot(
      0, serializerFactory)).isCompatibleAfterMigration shouldBe true
    versionedCustomSerializerSnapshot.resolveSchemaCompatibility(new VersionedCustomSerializerSnapshot(2, serializerFactory)).isIncompatible shouldBe true
    val unrelated = TypeInformation.of(classOf[String]).createSerializer(new ExecutionConfig()).snapshotConfiguration()
    versionedCustomSerializerSnapshot.resolveSchemaCompatibility(unrelated).isIncompatible shouldBe true
  }
}

class MyTypeInfo extends BaseTypeInfo[String](() => new MyTypeSerializer(2))

class MyTypeSerializer(readVersion: Int) extends TypeSerializerBase[String](readVersion) {
  override def deserializeRecord()(implicit source: DataInputView): String = ???
  override def serializeRecord(record: String)(implicit output: DataOutputView): Unit = ???
  override def snapshotConfiguration(): TypeSerializerSnapshot[String] = ???
}
