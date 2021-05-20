package org.wikidata.query.rdf.updater.config

case class StateExtractionConfig(args: Array[String]) extends BaseConfig()(BaseConfig.params(args)) {
  val jobName: String = params.get("job_name", "Streaming updater state extraction job (batch)")
  val inputSavepoint: String = getStringParam("input_savepoint")
  val outputRevisionPath: Option[String] = optionalStringArg("rev_map_output")
  val verify: Boolean = params.getBoolean("verify", false)
  val outputKafkaOffsets: Option[String] = optionalStringArg("kafka_offsets_output")
  val inputKafkaTopics: Option[InputKafkaTopics] = if (outputKafkaOffsets.isDefined) Some(getInputKafkaTopics) else None
}
