package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

object UpdaterStateDescriptors {
  def newLastRevisionStateDesc(): ValueStateDescriptor[java.lang.Long] = {
    new ValueStateDescriptor[java.lang.Long]("lastSeenRev", createTypeInformation[java.lang.Long])
  }
  def newReorderingStateDesc(): ListStateDescriptor[InputEvent] = {
    new ListStateDescriptor[InputEvent]("re-ordering-state", TypeInformation.of(classOf[InputEvent]))
  }
}
