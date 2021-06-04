package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
 * Serializer class for InputEvent.
 * Always writes the latest binary format but reads the format under readVersion.
 * In other words if the readVersion is different than the latest version this serializer
 * is asymmetric and requires always using TypeSerializerSchemaCompatibility.compatibleAfterMigration.
 *
 * Versions:
 * - V1 works with helper V1.
 */
object InputEventSerializer {
  val V1: Int = 1
  val currentVersion: Int = V1
  def typeInfo(): TypeInformation[InputEvent] = new BaseTypeInfo(() => new InputEventSerializer(currentVersion))
}

class InputEventSerializer(readVersion: Int) extends TypeSerializerBase[InputEvent](readVersion) {
  private val serializeHelper = helperForVersion(readVersion)

  def helperForVersion(readVersion: Int): SerializerHelper = {
    val helperVersion: Int = readVersion match {
      case InputEventSerializer.V1 => SerializerHelper.V1
      case _ => throw new IllegalArgumentException("Unsupported InputEventSerializer version " + readVersion)
    }
    new SerializerHelper(helperVersion)
  }

  def serializeRecord(record: InputEvent)(implicit output: DataOutputView): Unit = {
    serializeHelper.writeBasicEventData(record)
    record match {
      case revCreate: RevCreate =>
        output.writeUTF("RevCreate")
        serializeHelper.writeOptionalLong(revCreate.parentRevision)
      case _: PageDelete => output.writeUTF("PageDelete")
      case _: PageUndelete => output.writeUTF("PageUndelete")
    }
  }

  def deserializeRecord()(implicit input: DataInputView): InputEvent = {
    val (item, eventTime, ingestionTime, revision, eventInfo) = serializeHelper.readBasicEventData()
    input.readUTF() match {
      case "RevCreate" => RevCreate(item, eventTime, revision, serializeHelper.readOptionalLong(), ingestionTime, eventInfo)
      case "PageDelete" => PageDelete(item, eventTime, revision, ingestionTime, eventInfo)
      case "PageUndelete" => PageUndelete(item, eventTime, revision, ingestionTime, eventInfo)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[InputEvent] = new InputEventSerializerSnapshot()

  override def canEqual(other: Any): Boolean = other.isInstanceOf[InputEventSerializer]

  override def equals(other: Any): Boolean = other match {
    case that: InputEventSerializer =>
      (that canEqual this) &&
        serializeHelper == that.serializeHelper
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(serializeHelper.hashCode(), super.hashCode())
    state.foldLeft(0)((a, b) => 31 * a + b)
  }
}

class InputEventSerializerSnapshot extends VersionedCustomSerializerSnapshot[InputEvent](version => new InputEventSerializer(version))
