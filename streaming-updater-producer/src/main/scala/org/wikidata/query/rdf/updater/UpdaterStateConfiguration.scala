package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend

object UpdaterStateConfiguration {
  def newLastRevisionStateDesc(): ValueStateDescriptor[java.lang.Long] = {
    new ValueStateDescriptor[java.lang.Long]("lastSeenRev", createTypeInformation[java.lang.Long])
  }
  def newPartialReorderingStateDesc(useVersionedSerializers: Boolean): ListStateDescriptor[InputEvent] = {
    new ListStateDescriptor[InputEvent]("partial-re-ordering-state",
      if (useVersionedSerializers) {
        InputEventSerializer.typeInfo()
      } else {
        createTypeInformation[InputEvent]
      })
  }
  def newStateBackend(checkpointDir: String, enableIncrementalCheckpoint: Boolean = true): StateBackend = {
    new RocksDBStateBackend(checkpointDir, enableIncrementalCheckpoint)
  }
}
