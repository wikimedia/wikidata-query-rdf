package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.reflect.ClassTag

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.updater.SerializerHelper.V1

abstract class TypeSerializerBase[E](val readVersion: Int) extends TypeSerializer[E] {
  override final def isImmutableType: Boolean = true
  override final def duplicate(): TypeSerializer[E] = this
  override final def createInstance(): E = None.orNull.asInstanceOf[E]
  override final def copy(from: E): E = from
  override final def copy(from: E, reuse: E): E = from
  override final def getLength: Int = -1
  override final def deserialize(reuse: E, source: DataInputView): E = deserialize(source)
  override final def copy(source: DataInputView, target: DataOutputView): Unit = serialize(deserialize(source), target)
  override final def serialize(record: E, target: DataOutputView): Unit = {
    implicit val output: DataOutputView = target
    serializeRecord(record)
  }
  override def deserialize(source: DataInputView): E = {
    implicit val input: DataInputView = source
    deserializeRecord()
  }
  def deserializeRecord()(implicit source: DataInputView): E
  def serializeRecord(record: E)(implicit output: DataOutputView): Unit

  override final def toString: String = {
    getClass.getSimpleName + s"(readVersion:$readVersion)"
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[TypeSerializerBase[E]]

  override def equals(other: Any): Boolean = other match {
    case that: TypeSerializerBase[_] =>
      (that canEqual this) &&
        readVersion == that.readVersion
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(readVersion)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object SerializerHelper {
  val V1: Int = 1;
}

class SerializerHelper (val readVersion: Int) extends Serializable {
  assert(readVersion == V1) // readVersion is a placeholder for future usage

  def writeOptionalString(str: Option[String])(implicit output: DataOutputView): Unit = {
    writeOptional(str, output.writeUTF)
  }

  def writeOptionalLong(value: Option[Long])(implicit output: DataOutputView): Unit = {
    writeOptional(value, output.writeLong)
  }

  def writeInstant(instant: Instant)(implicit output: DataOutputView): Unit = {
    output.writeLong(instant.toEpochMilli)
  }

  def writeOptionalInstant(instant: Option[Instant])(implicit output: DataOutputView): Unit = {
    writeOptional(instant.map(_.toEpochMilli), output.writeLong)
  }

  def writeOptional[E](data: Option[E], writer: E => Unit)(implicit output: DataOutputView): Unit = {
    output.writeBoolean(data.isDefined)
    data.foreach(writer)
  }

  def writeBasicEventData(record: BasicEventData)(implicit output: DataOutputView): Unit = {
    output.writeUTF(record.item)
    writeInstant(record.eventTime)
    writeInstant(record.ingestionTime)
    output.writeLong(record.revision)
    serializeEventInfo(record.originalEventInfo)
  }

  def readOptionalString()(implicit input: DataInputView): Option[String] = {
    readOptional(input.readUTF)
  }

  def readInstant()(implicit input: DataInputView): Instant = {
    Instant.ofEpochMilli(input.readLong())
  }

  def readOptionalInstant()(implicit input: DataInputView): Option[Instant] = {
    readOptional(() => Instant.ofEpochMilli(input.readLong()))
  }

  def readOptional[E](reader: () => E)(implicit input: DataInputView): Option[E] = {
    Option(input.readBoolean())
      .filter(_.booleanValue())
      .map(_ => reader.apply())
  }

  def readOptionalLong()(implicit input: DataInputView): Option[Long] = {
    Option(input.readBoolean())
      .filter(_.booleanValue())
      .map(_ => input.readLong())
  }

  def readBasicEventData()(implicit input: DataInputView): (String, Instant, Instant, Long, EventInfo) = {
    val item = input.readUTF()
    val eventTime = readInstant()
    val ingestionTime = readInstant()
    val revision = input.readLong()
    val eventInfo = deserializeEventInfo()
    (item, eventTime, ingestionTime, revision, eventInfo)
  }


  def serializeEventInfo(record: EventInfo)(implicit output: DataOutputView): Unit = {
    writeInstant(record.meta().timestamp())
    writeOptionalString(Option(record.meta().id()))
    writeOptionalString(Option(record.meta().domain()))
    output.writeUTF(record.meta().stream())
    writeOptionalString(Option(record.meta().requestId()))
    output.writeUTF(record.schema())
  }

  def deserializeEventInfo()(implicit source: DataInputView): EventInfo = {
    new EventInfo(
      new EventsMeta(
        readInstant(), // timestamp
        readOptionalString().orNull, // id
        readOptionalString().orNull, // domain
        source.readUTF(), // stream
        readOptionalString().orNull), // request_id
      source.readUTF()) // schema
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SerializerHelper]

  override def equals(other: Any): Boolean = other match {
    case that: SerializerHelper =>
      (that canEqual this) &&
        readVersion == that.readVersion
    case _ => false
  }

  override def hashCode(): Int = {
    val state: Seq[Int] = Seq(readVersion)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}

class VersionedCustomSerializerSnapshot[E](serializerSupplier: Int => TypeSerializerBase[E]) extends TypeSerializerSnapshot[E] {
  private var version = -1

  override def getCurrentVersion: Int = 1

  override def writeSnapshot(out: DataOutputView): Unit = {}

  override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    version = readVersion
  }

  override def restoreSerializer(): TypeSerializer[E] = {
    serializerSupplier.apply(version)
  }

  override def resolveSchemaCompatibility(newSerializer: TypeSerializer[E]): TypeSerializerSchemaCompatibility[E] = {
    newSerializer match {
      case ser: TypeSerializerBase[E] => ser.readVersion match {
        case newV: Int if newV == version => TypeSerializerSchemaCompatibility.compatibleAsIs()
        case newV: Int if newV > version => TypeSerializerSchemaCompatibility.compatibleAfterMigration()
        case newV: Int if newV < version => TypeSerializerSchemaCompatibility.incompatible()
      }
      case _ => TypeSerializerSchemaCompatibility.incompatible()
    }
  }
}

class BaseTypeInfo[E: ClassTag](val serializer: () => TypeSerializerBase[E]) extends TypeInformation[E] {
  private lazy val serializerCache = serializer()
  override def isBasicType: Boolean = false
  override def isTupleType: Boolean = false
  override def getArity: Int = 1
  override def getTotalFields: Int = 1
  override def getTypeClass: Class[E] = reflect.classTag[E].runtimeClass.asInstanceOf[Class[E]]
  override def isKeyType: Boolean = false
  override def createSerializer(config: ExecutionConfig): TypeSerializer[E] = serializerCache

  def canEqual(other: Any): Boolean = other.isInstanceOf[BaseTypeInfo[E]]

  override def equals(other: Any): Boolean = other match {
    case that: BaseTypeInfo[E] =>
      (that canEqual this) &&
        serializerCache == that.serializerCache
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(serializerCache)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    getClass.getSimpleName + "[" + reflect.classTag[E].runtimeClass.getSimpleName + "]"
  }
}
