package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent
import org.wikidata.query.rdf.tool.MapperUtils

class RevisionCreateEventJson extends DeserializationSchema[RevisionCreateEvent] with SerializationSchema[RevisionCreateEvent] {
  override def deserialize(bytes: Array[Byte]): RevisionCreateEvent = MapperUtils.getObjectMapper.readValue(bytes, classOf[RevisionCreateEvent])
  override def isEndOfStream(t: RevisionCreateEvent): Boolean = false
  override def serialize(t: RevisionCreateEvent): Array[Byte] = MapperUtils.getObjectMapper.writeValueAsBytes(t)
  override def getProducedType: TypeInformation[RevisionCreateEvent] = TypeInformation.of(classOf[RevisionCreateEvent])
}
