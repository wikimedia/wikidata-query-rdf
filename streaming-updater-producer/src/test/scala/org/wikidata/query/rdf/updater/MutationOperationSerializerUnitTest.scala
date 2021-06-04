package org.wikidata.query.rdf.updater

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.nio.file.Files
import java.time.Instant

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.util.InstantiationUtil
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}

class MutationOperationSerializerUnitTest extends FlatSpec with Matchers {
  private val writeFixtureToTmp = false

  private val fullImport = FullImport("Q1", Instant.ofEpochMilli(1), 2, Instant.ofEpochMilli(11),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(111), "ID_1", "domain", "stream", "req_id"), "schema"))
  private val diff = Diff("Q1", Instant.ofEpochMilli(2), 3, 2, Instant.ofEpochMilli(22),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(222), "ID_2", "domain", "stream", "req_id"), "schema"))
  private val delete = DeleteItem("Q1", Instant.ofEpochMilli(3), 3, Instant.ofEpochMilli(33),
    new EventInfo(new EventsMeta(Instant.ofEpochMilli(333), "ID_3", "domain", "stream", "req_id"), "schema"))

  "MutationOperationSerializer" should "read/write current version" in {
    val serializer = MutationOperationSerializer.typeInfo().createSerializer(new ExecutionConfig())
    val snapshotConfig = serializer.snapshotConfiguration()
    val baos = new ByteArrayOutputStream()
    val output: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
    output.writeUTF(snapshotConfig.getClass.getName)
    output.writeInt(snapshotConfig.getCurrentVersion)
    snapshotConfig.writeSnapshot(output)
    serializer.serialize(fullImport, output)
    serializer.serialize(diff, output)
    serializer.serialize(delete, output)
    output.close()
    if (writeFixtureToTmp) {
      Files.write(new File("/tmp/MutationOperationSerializer.v" + MutationOperationSerializer.currentVersion + ".bin").toPath, baos.toByteArray)
    }

    val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(new ByteArrayInputStream(baos.toByteArray))
    val serSnapshot: TypeSerializerSnapshot[MutationOperation] = InstantiationUtil.instantiate(InstantiationUtil.resolveClassByName(input,
      this.getClass.getClassLoader, classOf[TypeSerializerSnapshot[MutationOperation]]))
    serSnapshot.readSnapshot(input.readInt(), input, this.getClass.getClassLoader)
    val deser = serSnapshot.restoreSerializer()
    deser shouldEqual serializer
    deser.deserialize(input) shouldBe fullImport
    deser.deserialize(input) shouldBe diff
    deser.deserialize(input) shouldBe delete
  }

  "MutationOperationSerializer V1 format" should "be readable" in {
    val input: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(this.getClass.getResourceAsStream("MutationOperationSerializer.v1.bin"))
    val serSnapshot: TypeSerializerSnapshot[MutationOperation] = InstantiationUtil.instantiate(InstantiationUtil.resolveClassByName(input,
      this.getClass.getClassLoader, classOf[TypeSerializerSnapshot[MutationOperation]]))
    serSnapshot.readSnapshot(input.readInt(), input, this.getClass.getClassLoader)
    val deser = serSnapshot.restoreSerializer()
    deser shouldBe a[MutationOperationSerializer]
    deser.asInstanceOf[MutationOperationSerializer].readVersion shouldBe 1
    deser.deserialize(input) shouldBe fullImport
    deser.deserialize(input) shouldBe diff
    deser.deserialize(input) shouldBe delete
  }
}
