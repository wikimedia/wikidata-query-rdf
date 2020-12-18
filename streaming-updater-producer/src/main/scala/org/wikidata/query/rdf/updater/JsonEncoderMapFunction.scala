package org.wikidata.query.rdf.updater

import java.io.StringWriter
import java.time.Instant

import com.fasterxml.jackson.core.JsonGenerator
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.wikidata.query.rdf.tool.MapperUtils

class JsonEncoderMapFunction[E](serializer: (E, Instant, JsonGenerator) => String) extends ProcessFunction[E, String] {

  override def processElement(i: E, context: ProcessFunction[E, String]#Context, collector: Collector[String]): Unit = {
    val writer = new StringWriter()
    val generator = MapperUtils.getObjectMapper.getFactory.createGenerator(writer)
    serializer.apply(i, Instant.ofEpochMilli(context.timerService().currentProcessingTime()), generator);

  }
}
