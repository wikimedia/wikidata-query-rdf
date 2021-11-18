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
object MutationOperationSerializer {
  val V1: Int = 1
  val V2: Int = 2
  val currentVersion: Int = V2
  def typeInfo(): TypeInformation[MutationOperation] = new BaseTypeInfo(() => new MutationOperationSerializer(currentVersion))
}

class MutationOperationSerializer(readVersion: Int) extends TypeSerializerBase[MutationOperation](readVersion) {
  private val serializeHelper = helperForVersion(readVersion)
  def helperForVersion(readVersion: Int): SerializerHelper = {
    val helperVersion: Int = readVersion match {
      case MutationOperationSerializer.V1 => SerializerHelper.V1
      case MutationOperationSerializer.V2 => SerializerHelper.V1
      case _ => throw new IllegalArgumentException("Unsupported InputEventSerializer version " + readVersion)
    }
    new SerializerHelper(helperVersion)
  }

  def serializeRecord(record: MutationOperation)(implicit output: DataOutputView): Unit = {
    serializeHelper.writeBasicEventData(record)
    record match {
      case diff: Diff =>
        output.writeUTF("Diff")
        output.writeLong(diff.fromRev)
      case _: FullImport => output.writeUTF("FullImport")
      case _: DeleteItem => output.writeUTF("DeleteItem")
      case _: Reconcile => output.writeUTF("Reconcile")
    }
  }

  def deserializeRecord()(implicit input: DataInputView): MutationOperation = {
    val (item, eventTime, ingestionTime, revision, eventInfo) = serializeHelper.readBasicEventData()
    input.readUTF() match {
      case "Diff" => Diff(item, eventTime, revision, input.readLong(), ingestionTime, eventInfo)
      case "FullImport" => FullImport(item, eventTime, revision, ingestionTime, eventInfo)
      case "DeleteItem" => DeleteItem(item, eventTime, revision, ingestionTime, eventInfo)
      case "Reconcile" => Reconcile(item, eventTime, revision, ingestionTime, eventInfo)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[MutationOperation] = new MutationOptionSerializerSnapshot()

  override def canEqual(other: Any): Boolean = other.isInstanceOf[MutationOperationSerializer]

  override def equals(other: Any): Boolean = other match {
    case that: MutationOperationSerializer =>
      (that canEqual this) &&
        super.equals(other) &&
        serializeHelper == that.serializeHelper
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(serializeHelper.hashCode(), super.hashCode())
    state.foldLeft(0)((a, b) => 31 * a + b)
  }
}

class MutationOptionSerializerSnapshot
  extends VersionedCustomSerializerSnapshot(
    MutationOperationSerializer.currentVersion,
    v => new MutationOperationSerializer(v)
  )



