package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend

object UpdaterStateConfiguration {
  def newLastRevisionStateDesc(): ValueStateDescriptor[java.lang.Long] = {
    new ValueStateDescriptor[java.lang.Long]("lastSeenRev", createTypeInformation[java.lang.Long])
  }
  def newPartialReorderingStateDesc(): ListStateDescriptor[InputEvent] = {
    new ListStateDescriptor[InputEvent]("partial-re-ordering-state", InputEventSerializer.typeInfo())
  }
  def newStateBackend(enableIncrementalCheckpoint: Boolean = true): StateBackend = {
    // FIXME only here so the project can be built on ARM64
    if (System.getProperty("os.arch").equals("aarch64")) new HashMapStateBackend() else new EmbeddedRocksDBStateBackend(enableIncrementalCheckpoint)
  }
}
