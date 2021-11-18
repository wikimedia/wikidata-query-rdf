package org.wikidata.query.rdf.updater

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.nio.file.Files
import java.time.Instant

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputView, DataOutputViewStreamWrapper}
import org.apache.flink.util.InstantiationUtil
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}

class InputEventSerializerUnitTest extends FlatSpec with Matchers {
  private val writeFixtureToTmp = false
  private val revCreate1 = RevCreate("Q1", Instant.ofEpochMilli(1), 2, None, Instant.ofEpochMilli(11),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(111), "ID_1", "domain", "stream", "req_id"), "schema_rev_create"))
  private val revCreate2 = RevCreate("Q1", Instant.ofEpochMilli(2), 3, Some(2), Instant.ofEpochMilli(22),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(222), "ID_2", "domain", "stream", "req_id"), "schema_rev_create"))
  private val pageDelete = PageDelete("Q1", Instant.ofEpochMilli(3), 3, Instant.ofEpochMilli(33),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(333), "ID_3", "domain", "stream", "req_id"), "schema_page_delete"))
  private val pageUndelete = PageUndelete("Q1", Instant.ofEpochMilli(4), 3, Instant.ofEpochMilli(44),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(444), "ID_4", "domain", "stream", "req_id"), "schema_page_undelete"))
  private val reconcileCreation = ReconcileInputEvent("Q1", Instant.ofEpochMilli(4), 3, ReconcileCreation, Instant.ofEpochMilli(55),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(555), "ID_5", "domain", "stream", "req_id"), "schema_updater_reconcile"))
  private val reconcileDeletion = ReconcileInputEvent("Q1", Instant.ofEpochMilli(4), 3, ReconcileDeletion, Instant.ofEpochMilli(66),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(666), "ID_5", "domain", "stream", "req_id"), "schema_updater_reconcile"))

  "InputEventSerializer" should "read/write current version" in {
    val serializer = InputEventSerializer.typeInfo().createSerializer(new ExecutionConfig())
    val snapshotConfig = serializer.snapshotConfiguration()
    val baos = new ByteArrayOutputStream()
    val output: DataOutputView = new DataOutputViewStreamWrapper(baos)
    output.writeUTF(snapshotConfig.getClass.getName)
    output.writeInt(snapshotConfig.getCurrentVersion)
    snapshotConfig.writeSnapshot(output)
    serializer.serialize(revCreate1, output)
    serializer.serialize(revCreate2, output)
    serializer.serialize(pageDelete, output)
    serializer.serialize(pageUndelete, output)
    serializer.serialize(reconcileCreation, output)
    serializer.serialize(reconcileDeletion, output)
    baos.close()
    if (writeFixtureToTmp) {
      Files.write(new File("/tmp/InputEventSerializer.v" + InputEventSerializer.currentVersion + ".bin").toPath, baos.toByteArray)
    }

    val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(new ByteArrayInputStream(baos.toByteArray))
    val serSnapshot: TypeSerializerSnapshot[InputEvent] = InstantiationUtil.instantiate(InstantiationUtil.resolveClassByName(input,
      this.getClass.getClassLoader, classOf[TypeSerializerSnapshot[InputEvent]]))
    serSnapshot.readSnapshot(input.readInt(), input, this.getClass.getClassLoader)
    val deser = serSnapshot.restoreSerializer()
    deser shouldEqual serializer
    deser.deserialize(input) shouldBe revCreate1
    deser.deserialize(input) shouldBe revCreate2
    deser.deserialize(input) shouldBe pageDelete
    deser.deserialize(input) shouldBe pageUndelete
    deser.deserialize(input) shouldBe reconcileCreation
    deser.deserialize(input) shouldBe reconcileDeletion
  }

  "InputEventSerializer V1 format" should "be readable" in {
    val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(this.getClass.getResourceAsStream("InputEventSerializer.v1.bin"))
    val serSnapshot: TypeSerializerSnapshot[InputEvent] = InstantiationUtil.instantiate(InstantiationUtil.resolveClassByName(input,
      this.getClass.getClassLoader, classOf[TypeSerializerSnapshot[InputEvent]]))
    serSnapshot.readSnapshot(input.readInt(), input, this.getClass.getClassLoader)
    val deser = serSnapshot.restoreSerializer()
    deser shouldBe a[InputEventSerializer]
    deser.asInstanceOf[InputEventSerializer].readVersion shouldBe 1
    deser.deserialize(input) shouldBe revCreate1
    deser.deserialize(input) shouldBe revCreate2
    deser.deserialize(input) shouldBe pageDelete
    deser.deserialize(input) shouldBe pageUndelete
  }

  "InputEventSerializer V2 format" should "be readable" in {
    val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(this.getClass.getResourceAsStream("InputEventSerializer.v2.bin"))
    val serSnapshot: TypeSerializerSnapshot[InputEvent] = InstantiationUtil.instantiate(InstantiationUtil.resolveClassByName(input,
      this.getClass.getClassLoader, classOf[TypeSerializerSnapshot[InputEvent]]))
    serSnapshot.readSnapshot(input.readInt(), input, this.getClass.getClassLoader)
    val deser = serSnapshot.restoreSerializer()
    deser shouldBe a[InputEventSerializer]
    deser.asInstanceOf[InputEventSerializer].readVersion shouldBe 2
    deser.deserialize(input) shouldBe revCreate1
    deser.deserialize(input) shouldBe revCreate2
    deser.deserialize(input) shouldBe pageDelete
    deser.deserialize(input) shouldBe pageUndelete
    deser.deserialize(input) shouldBe reconcileCreation
    deser.deserialize(input) shouldBe reconcileDeletion
  }
}
