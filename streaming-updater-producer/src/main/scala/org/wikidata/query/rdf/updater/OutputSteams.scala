package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.scala.DataStream

case class OutputStreams(
                          mutationSink: SinkWrapper[MutationDataChunk],
                          lateEventsSink: Option[SinkWrapper[InputEvent]] = None,
                          spuriousEventsSink: Option[SinkWrapper[InconsistentMutation]] = None,
                          failedOpsSink: Option[SinkWrapper[FailedOp]] = None
                        )

trait SinkWrapper[E] {
  def attachStream(stream: DataStream[E]): Unit
}


