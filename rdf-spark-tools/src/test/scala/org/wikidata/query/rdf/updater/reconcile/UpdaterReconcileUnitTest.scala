package org.wikidata.query.rdf.updater.reconcile

import java.{lang, util}
import java.net.URI
import java.util.Optional

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.spark.SparkSessionProvider
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent.Action
import org.wikidata.query.rdf.tool.EntityId
import org.wikidata.query.rdf.updater.reconcile.UpdaterReconcile.Params

class UpdaterReconcileUnitTest extends AnyFlatSpec with SparkSessionProvider with Matchers with MockFactory {
  val latestRevForItems: util.Set[EntityId] => util.Map[EntityId, Optional[lang.Long]] = stub[util.Set[EntityId] => util.Map[EntityId, Optional[lang.Long]]]
  val latestRevForMediaInfoItems: util.Set[EntityId] => util.Map[EntityId, Optional[lang.Long]] =
    stub[util.Set[EntityId] => util.Map[EntityId, Optional[lang.Long]]]

  "ReconcileCollector" should "collect late events" in {
    val df = spark.read.json(this.getClass.getResource("late-events.json").toURI.toString)
    df.createTempView("late_events")
    val collector = new ReconcileCollector("my_source@$DC$@$EMITTER_ID$", "my_stream", "www.wikidata.org", latestRevForItems, latestRevForMediaInfoItems)
    val events = collector.collectLateEvents("late_events/year=2021/month=11")
    val noData = collector.collectLateEvents("late_events/year=2021/month=11/datacenter=unknownDC")
    val eqiadEvents = events.filter(_.getReconciliationSource.endsWith("eqiad@my_id"))
    val codfwEvents = events.filter(_.getReconciliationSource.endsWith("codfw@my_id"))
    codfwEvents should have size 1
    eqiadEvents should have size 1
    noData shouldBe empty
    eqiadEvents.head.getItem.toString shouldBe "Q894254"
    codfwEvents.head.getItem.toString shouldBe "Q894254"
    eqiadEvents.head.getReconciliationAction shouldBe Action.CREATION
    codfwEvents.head.getReconciliationAction shouldBe Action.CREATION
    eqiadEvents map { _.getReconciliationSource } should contain only "my_source@eqiad@my_id"
    codfwEvents map { _.getReconciliationSource } should contain only "my_source@codfw@my_id"
  }

  "ReconcileCollector" should "collect inconsistencies" in {
    val df = spark.read.json(this.getClass.getResource("inconsistencies.json").toURI.toString)
    df.createTempView("inconsistencies")
    val collector = new ReconcileCollector("my_source@$DC$@$EMITTER_ID$", "my_stream", "www.wikidata.org", latestRevForItems, latestRevForMediaInfoItems)
    val events = collector.collectInconsistencies("inconsistencies/year=2021/month=11")
    val eqiadEvents = events.filter(_.getReconciliationSource.endsWith("eqiad@my_id"))
    val codfwEvents = events.filter(_.getReconciliationSource.endsWith("codfw@my_id"))
    codfwEvents should have size 10
    eqiadEvents should have size 4
    codfwEvents map { _.getItem.toString } should contain only ("Q101208968", "Q106605647", "Q1437663", "Q108922819",
      "Q109332244", "Q109616453", "Q109658637", "Q17370984", "Q87538978", "Q123")

    codfwEvents map { _.getReconciliationAction } should contain only Action.CREATION
    codfwEvents map { _.getReconciliationSource } should contain only "my_source@codfw@my_id"
  }

  "ReconcileCollector" should "collect failures" in {
    val df = spark.read.json(this.getClass.getResource("failures.json").toURI.toString)

    val revsForEqiad: util.Map[EntityId, Optional[lang.Long]] = Map(
      EntityId.parse("L620507") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q97495350") -> Optional.empty[lang.Long](),
      EntityId.parse("Q89138924") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q44411127") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q41644871") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q41628488") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q108906915") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q65786717") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q28885212") -> Optional.of(long2Long(Long.MaxValue))
    ).asJava

    val revsForCodfw: util.Map[EntityId, Optional[lang.Long]] = Map(
      EntityId.parse("L620507") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q108906915") -> Optional.empty[lang.Long](),
      EntityId.parse("Q109680063") -> Optional.of(long2Long(1L)),
      EntityId.parse("Q109768229") -> Optional.of(long2Long(Long.MaxValue))
    ).asJava

    val revsForMediaInfo: util.Map[EntityId, Optional[lang.Long]] = Map(
      EntityId.parse("M91170167") -> Optional.of(long2Long(1L)),
      EntityId.parse("M107454021") -> Optional.empty[lang.Long](),
      EntityId.parse("M82223288") -> Optional.of(long2Long(1L)),
      EntityId.parse("M106386043") -> Optional.of(long2Long(Long.MaxValue))
    ).asJava

    (latestRevForItems apply _) when revsForEqiad.keySet() returns revsForEqiad
    (latestRevForItems apply _) when revsForCodfw.keySet() returns revsForCodfw
    (latestRevForMediaInfoItems apply _) when revsForMediaInfo.keySet() returns revsForMediaInfo

    df.createTempView("failures")
    val collector = new ReconcileCollector("my_source@$DC$@$EMITTER_ID$", "my_stream", "www.wikidata.org", latestRevForItems, latestRevForMediaInfoItems)
    val events = collector.collectFailures("failures/year=2021/month=11")

    val mediaInfoCollector = new ReconcileCollector("my_mediainfo_source@$DC$", "my_stream", "commons.wikimedia.org",
      latestRevForItems, latestRevForMediaInfoItems)
    val allMediaInfoEvents = mediaInfoCollector.collectFailures("failures/year=2021/month=12")

    val eqiadEvents = events.filter(_.getReconciliationSource.endsWith("eqiad@my_id"))
    val codfwEvents = events.filter(_.getReconciliationSource.endsWith("codfw@my_id"))
    val mediaInfoEvents = allMediaInfoEvents.filter(_.getReconciliationSource.endsWith("eqiad"))

    eqiadEvents map { _.getItem.toString } should contain allOf("L620507", "Q89138924", "Q44411127", "Q41644871",
      "Q41628488", "Q108906915", "Q65786717", "Q28885212")
    codfwEvents map { _.getItem.toString } should contain allOf("L620507", "Q109680063", "Q109768229")
    mediaInfoEvents map { _.getItem.toString } should contain allOf("M106386043", "M82223288", "M91170167")

    eqiadEvents map { _.getRevision } should contain(Long.MaxValue)
    codfwEvents map { _.getRevision } should contain(Long.MaxValue)
    mediaInfoEvents map { _.getRevision } should contain(Long.MaxValue)

    eqiadEvents map { _.getReconciliationSource } should contain only "my_source@eqiad@my_id"
    codfwEvents map { _.getReconciliationSource } should contain only "my_source@codfw@my_id"
    mediaInfoEvents map { _.getReconciliationSource } should contain only "my_mediainfo_source@eqiad"

    codfwEvents map { _.getReconciliationAction } should contain only Action.CREATION
    mediaInfoEvents map { _.getReconciliationAction } should contain only Action.CREATION
  }

  "UpdaterReconcile" should "properly parse cmdline arguments" in {
    val mandatoryParams = Seq(
      "--domain", "my.domain.local",
      "--reconciliation-source", "my_reconciliation_source_name",
      "--event-gate", "my.eventgate.local",
      "--late-events-partition", "late-events/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      "--inconsistencies-partition", "inconsistencies/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      "--failures-partition", "failures/y=2022/m=1/d=1/h=0/datacenter=eqiad"
    )
    UpdaterReconcile.argsParser.parse(mandatoryParams, UpdaterReconcile.Params()) shouldBe Some(Params(
      domain = "my.domain.local",
      reconciliationSource = "my_reconciliation_source_name",
      eventGateEndpoint = URI.create("my.eventgate.local"),
      lateEventPartitionSpec = "late-events/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      inconsistenciesPartitionSpec = "inconsistencies/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      failuresPartitionSpec = "failures/y=2022/m=1/d=1/h=0/datacenter=eqiad"
    ))

    val optionalParams = Seq(
      "--stream", "my-stream",
      "--entity-namespaces", "0,1,2,3",
      "--api-path", "/w/my-api.php",
      "--entity-data-path", "/wiki/Special:MyEntityData",
      "--initial-to-namespace", "Q=,L=1,P=2,M=3",
      "--http-routes", "my.domain.local=http://127.0.0.1:1234,my.eventgate.local=http://127.0.0.1:1235",
      "--http-request-timeout", "10"
    )

    UpdaterReconcile.argsParser.parse(mandatoryParams ++ optionalParams, UpdaterReconcile.Params()) shouldBe Some(Params(
      domain = "my.domain.local",
      reconciliationSource = "my_reconciliation_source_name",
      eventGateEndpoint = URI.create("my.eventgate.local"),
      lateEventPartitionSpec = "late-events/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      inconsistenciesPartitionSpec = "inconsistencies/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      failuresPartitionSpec = "failures/y=2022/m=1/d=1/h=0/datacenter=eqiad",
      stream = "my-stream",
      entityNamespaces = Seq(0, 1, 2, 3),
      initialToNamespace = "Q=,L=1,P=2,M=3",
      apiPath = "/w/my-api.php",
      entityDataPath = "/wiki/Special:MyEntityData",
      httpRoutes = Some("my.domain.local=http://127.0.0.1:1234,my.eventgate.local=http://127.0.0.1:1235"),
      httpRequestTimeout = 10 seconds
    ))
  }
}
