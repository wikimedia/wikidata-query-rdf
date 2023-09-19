package org.wikidata.query.rdf.updater

import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala._
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.tool.EntityId
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent
import org.wikidata.query.rdf.updater.EntityStatus.CREATED
import org.wikidata.query.rdf.updater.config.{BaseConfig, BootstrapConfig, HttpClientConfig, UpdaterPipelineGeneralConfig}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps


class UpdaterBootstrapJobIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers with BeforeAndAfter {
  private var savePointDir: File = _
  private var checkPointDirInStream: File = _


  before {
    savePointDir = Files.createTempDirectory("savePoint").toFile
    checkPointDirInStream = Files.createTempDirectory("checkPointStream").toFile
  }

  "a savepoint" should "created by loading a csv file with entity revisions" in {
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // configure your test environment
    env.setParallelism(PARALLELISM)

    val csvFile = this.getClass.getResource("/bootstrap_revisions.csv").toString

    val config = BootstrapConfig(Seq[String](
    "--job_name", "bootstrap",
      "--revisions_file", csvFile,
      "--savepoint_dir", savePointDir.getAbsolutePath,
      "--parallelism", String.valueOf(PARALLELISM)
    ).toArray)
    UpdaterBootstrapJob.newSavepoint(config)
      .write(config.savepointDir)
    env.execute("write savepoint")
    val metatadaFile = Paths.get(savePointDir.getAbsolutePath, "_metadata").toFile
    metatadaFile.exists() should equal(true)

    implicit val streamingEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    streamingEnv.setParallelism(PARALLELISM)
    streamingEnv.setStateBackend(UpdaterStateConfiguration.newStateBackend())
    streamingEnv.getCheckpointConfig.setCheckpointStorage(checkPointDirInStream.toURI)
    streamingEnv.setMaxParallelism(BaseConfig.MAX_PARALLELISM)
    val repository: WikibaseEntityRevRepositoryTrait = MockWikibaseEntityRevRepository()
      .withResponse(("Q1", 2L) -> metaStatements("Q1", 2L, Some(3L)).entityDataNS)
      .withResponse(("Q1", 3L) -> metaStatements("Q1", 3L, Some(3L)).entityDataNS)
      .withResponse(("Q2", 4L) -> metaStatements("Q2", 4L, Some(3L)).entityDataNS)
      .withResponse(("Q2", 8L) -> metaStatements("Q2", 8L, Some(3L)).entityDataNS)
      .withResponse(("Q3", 101010L) -> metaStatements("Q3", 101010L, Some(3L)).entityDataNS)
      .withResponse(("Q3", 101013L) -> metaStatements("Q3", 101013L, Some(3L)).entityDataNS)

    val input = Seq(
      // dupped event, currently treated as spurious
      newRevCreateEvent(EntityId.parse("Q1"), 1, 2, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID, DEFAULT_REV_SLOTS),
      // normal events
      newRevCreateEvent(EntityId.parse("Q1"), 1, 3, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID, DEFAULT_REV_SLOTS),
      newRevCreateEvent(EntityId.parse("Q2"), 2, 8, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID, DEFAULT_REV_SLOTS),
      newRevCreateEvent(EntityId.parse("Q3"), 3, 101013, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID, DEFAULT_REV_SLOTS)
    )

    val resolver: IncomingStreams.EntityResolver = (_, title, _) => title
    val source: DataStream[InputEvent] = IncomingStreams.fromStream(streamingEnv.fromCollection(input)
      .assignTimestampsAndWatermarks(watermarkStrategy[RevisionCreateEvent]()),
      URIS,
      IncomingStreams.REV_CREATE_CONV,
      clock, resolver, None)

    val options = UpdaterPipelineGeneralConfig(
      hostname = DOMAIN,
      jobName = "test updater job",
      entityNamespaces = ENTITY_NAMESPACES,
      entityDataPath = "/Special:EntityData",
      reorderingWindowLengthMs = 60000,
      generateDiffTimeout = Int.MaxValue,
      wikibaseRepoThreadPoolSize = 10,
      httpClientConfig = HttpClientConfig(None, None, "my user-agent"),
      urisScheme = UrisSchemeFactory.forWikidataHost(DOMAIN),
      acceptableMediawikiLag = 10 seconds
    )
    UpdaterPipeline.configure(options, List(source),
      OutputStreams(
        SinkWrapper(Left(new CollectSink[MutationDataChunk](CollectSink.values.append(_))), "mutations"),
        SinkWrapper(Left(new CollectSink[InputEvent](CollectSink.lateEvents.append(_))), "late-events"),
        SinkWrapper(Left(new CollectSink[InconsistentMutation](CollectSink.spuriousRevEvents.append(_))), "inconsistencies"),
        SinkWrapper(Left(new DiscardingSink[FailedOp]()), "failures")
      ),
      _ => repository, clock = clock)
    val graph = streamingEnv.getStreamGraph(true)
    graph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savePointDir.toURI.toString, false))
    streamingEnv.getJavaEnv.execute(graph)
    CollectSink.lateEvents shouldBe empty
    CollectSink.spuriousRevEvents should contain only IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID)),
      instantNow, NewerRevisionSeen, State(Some(2), CREATED))
    //only change is the revision, lastmodified are identical

    CollectSink.values map {_.operation} should contain theSameElementsAs Vector(
      Diff("Q1", instant(3), 3, 2, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID)),
      Diff("Q2", instant(3), 8, 4, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID)),
      Diff("Q3", instant(3), 101013, 101010, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID))
    )
  }


  after {
    FileUtils.forceDelete(savePointDir)
    FileUtils.forceDelete(checkPointDirInStream)
  }
}
