package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.wikidata.query.rdf.tool.MapperUtils


object DeserializationSchemaFactory {
  def getDeserializationSchema[E](clazz: Class[E]): DeserializationSchema[E] = {
    new DeserializationSchema[E] {
      override def deserialize(bytes: Array[Byte]): E = MapperUtils.getObjectMapper.readValue(bytes, clazz)

      override def isEndOfStream(t: E): Boolean = false

      override def getProducedType: TypeInformation[E] = TypeInformation.of(clazz)
    }
  }
}
