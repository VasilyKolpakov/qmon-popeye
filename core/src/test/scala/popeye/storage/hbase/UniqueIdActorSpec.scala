package popeye.storage.hbase

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import akka.dispatch.ExecutionContexts
import popeye.test.MockitoStubs
import popeye.pipeline.test.AkkaTestKitSpec
import org.mockito.Mockito._
import akka.testkit.TestActorRef
import akka.actor.Props
import popeye.storage.hbase.UniqueIdProtocol.{ResolutionFailed, Resolved, FindName}
import popeye.storage.hbase.HBaseStorage.{QualifiedId, ResolvedName, QualifiedName}
import akka.pattern.ask
import scala.concurrent.{Promise, Future, Await}
import scala.concurrent.duration._
import akka.util.Timeout
import org.mockito.Matchers._


class UniqueIdActorSpec extends AkkaTestKitSpec("uniqueid") with MockitoStubs {
  implicit val timeout = Timeout(1 seconds)
  implicit val executionContext = system.dispatcher
  behavior of "UniqueIdActor"

  it should "create new unique id" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = createUniqueIdActor(storage)
    val generationId = new BytesKey(Array[Byte](0, 0))
    val qName = QualifiedName("kind", generationId, "name")
    val id: BytesKey = new BytesKey(Array[Byte](0))
    stub(storage.findByName(Seq(qName))).toReturn(Seq())
    stub(storage.registerName(qName)).toReturn(ResolvedName(qName, id))
    val responseFuture = actor ? FindName(qName, create = true)
    val response = Await.result(responseFuture, 5 seconds)
    verify(storage).findByName(Seq(qName))
    verify(storage).registerName(qName)
    verifyNoMoreInteractions(storage)
    response should equal(Resolved(ResolvedName(qName, id)))
  }

  it should "use batching" in {
    val numberOfNames = 10
    val allRequestsSent = Promise[Unit]()
    val maxBatchSize = new AtomicLong(0)
    val storage = new UniqueIdStorageStub {
      override def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = {
        if (!allRequestsSent.future.isCompleted) {
          Await.result(allRequestsSent.future, 5 seconds)
        }
        if (maxBatchSize.get() < qnames.size) {
          maxBatchSize.set(qnames.size)
        }
        qnames.map(qName => ResolvedName(qName, new BytesKey(Array())))
      }
    }
    val actor = createUniqueIdActor(storage)
    val generationId = new BytesKey(Array[Byte](0, 0))
    val qNames = (0 until numberOfNames).map(i => QualifiedName("kind", generationId, i.toString))
    val idFutures = qNames.toList.map {
      qName => actor ? FindName(qName)
    }
    allRequestsSent.success(())
    val responses = Await.result(Future.sequence(idFutures), 100 millis)
    responses.collect { case ResolutionFailed(t) => t }.headOption.foreach(throw _)
    maxBatchSize.get.toInt should (be > 1)
  }

  it should "handle failures" in {
    val storage = mock[UniqueIdStorageTrait]
    val actor = createUniqueIdActor(storage)
    val generationId = new BytesKey(Array[Byte](0, 0))
    val resolvedName = ResolvedName(QualifiedName("kind", generationId, "first"), new BytesKey(Array[Byte](0)))
    storage.findByName(any[Seq[QualifiedName]]) throws new RuntimeException thenAnswers (_ => Seq(resolvedName))
    actor ! FindName(resolvedName.toQualifiedName, create = true)
    val future = actor ? FindName(resolvedName.toQualifiedName, create = true)
    val response = Await.result(future, 5 seconds)
    response should equal(Resolved(resolvedName))
  }

  class UniqueIdStorageStub extends UniqueIdStorageTrait {
    override def findByName(qnames: Seq[QualifiedName]): Seq[ResolvedName] = ???

    override def findById(ids: Seq[QualifiedId]): Seq[ResolvedName] = ???

    override def registerName(qname: QualifiedName): ResolvedName = ???
  }

  def createUniqueIdActor(storage: UniqueIdStorageTrait): TestActorRef[Nothing] = {
    val exctx = ExecutionContexts.fromExecutor(Executors.newSingleThreadExecutor())
    TestActorRef(Props.apply(new UniqueIdActor(storage, exctx)))
  }
}
