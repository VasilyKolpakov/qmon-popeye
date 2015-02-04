package popeye

import java.util.concurrent.Executors

import akka.dispatch.ExecutionContexts
import org.scalatest.{Matchers, FlatSpec}
import AsyncIterator._

import scala.concurrent.duration._
import scala.concurrent.{Promise, Await, Future}

class AsyncIteratorSpec extends FlatSpec with Matchers {

  implicit val exct = ExecutionContexts.fromExecutor(Executors.newCachedThreadPool())

  behavior of "AsyncIterator.iterate"

  it should "create a singleton iterator" in {
    val iter = iterate(Future.successful(0)) {
      i => None
    }
    toSeq(iter) should equal(Seq(0))
  }

  it should "create a simple iterator" in {
    val iter = iterate(Future.successful(0)) {
      i =>
        if (i < 5) {
          Some(Future.successful(i + 1))
        } else {
          None
        }
    }
    toSeq(iter) should equal(Seq(0, 1, 2, 3, 4, 5))
  }

  behavior of "AsyncIterator.wihtReadahead"

  it should "make execution parallel" in {
    val firstIteratorIsDone = Promise[Unit]()
    val iter = iterate(Future.successful(0)) {
      i =>
        if (i <= 1) {
          Some(Future.successful(i + 1))
        } else {
          firstIteratorIsDone.success(())
          None
        }
    }.withReadahead(1)
    val mapped = iter.map {
      i =>
        if (i == 1) {
          Await.result(firstIteratorIsDone.future, Duration.Inf)
        }
        Future.successful(())
    }
    val foldLeftFuture = AsyncIterator.foldLeft[Unit, Unit](mapped, (), (_, _) => (), Promise().future)
    Await.result(foldLeftFuture, 5 seconds)
  }

  def toSeq[A](iter: AsyncIterator[A]) = {
    Await.result(toStrictSeq(iter), Duration.Inf)
  }
}
