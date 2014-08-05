package popeye.analytics

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object IteratorUtils {
  def mergeSorted[A](iterators: Seq[BufferedIterator[A]])(implicit cmp: Ordering[A]): Iterator[A] = {
    val iterOrdering = Ordering.by[BufferedIterator[A], A](_.head)(cmp.reverse)
    val queue = mutable.PriorityQueue[BufferedIterator[A]]()(iterOrdering)
    new MergeIterator[A](queue ++= iterators.filter(_.hasNext))
  }

  class MergeIterator[A](queue: mutable.PriorityQueue[BufferedIterator[A]]) extends Iterator[A] {

    override def hasNext: Boolean = queue.nonEmpty

    override def next(): A = {
      val iter = queue.dequeue()
      val nextItem = iter.next()
      if (iter.hasNext) {
        queue += iter
      }
      nextItem
    }
  }

  def uniqByOrdering[A](iter: Iterator[A])(implicit cmp: Ordering[A]): Iterator[A] = {
    if (iter.isEmpty) {
      iter
    } else {
      new UniqIterator[A](iter, cmp)
    }
  }

  class UniqIterator[A](iter: Iterator[A], cmp: Ordering[A]) extends Iterator[A] {

    var bufferedItem = iter.next()
    var buffItemIsSet = true

    override def hasNext: Boolean = buffItemIsSet

    override def next(): A = {
      val nextItem = bufferedItem
      if (iter.hasNext) {
        var item = iter.next()
        while(cmp.equiv(bufferedItem, item) && iter.hasNext) {
          item = iter.next()
        }
        if (!iter.hasNext && cmp.equiv(bufferedItem, item)) {
          buffItemIsSet = false
        } else {
          bufferedItem = item
        }
        nextItem
      } else {
        buffItemIsSet = false
        bufferedItem
      }
    }
  }

  def groupByPredicate[A](iter: Iterator[A], isRelated: (A, A) => Boolean): Iterator[ArrayBuffer[A]] = {
    if (iter.isEmpty) {
      Iterator.empty
    } else {
      new GroupingIterator[A](iter, isRelated)
    }
  }

  class GroupingIterator[A](iter: Iterator[A], isRelated: (A, A) => Boolean) extends Iterator[ArrayBuffer[A]] {

    var currentGroupItems = ArrayBuffer[A]()
    var currentGroupItem = {
      val item = iter.next()
      currentGroupItems += item
      item
    }

    override def hasNext: Boolean = currentGroupItems.nonEmpty

    override def next(): ArrayBuffer[A] = {
      if (iter.hasNext) {
        var item = iter.next()
        while(isRelated(currentGroupItem, item) && iter.hasNext) {
          currentGroupItems += item
          item = iter.next()
        }
        val group = currentGroupItems
        if (!iter.hasNext && isRelated(currentGroupItem, item)) {
          // last group
          currentGroupItems += item
          currentGroupItems = ArrayBuffer[A]()
        } else {
          // non last group
          currentGroupItems = ArrayBuffer[A](item)
        }
        currentGroupItem = item
        group
      } else {
        val group = currentGroupItems
        currentGroupItems = ArrayBuffer[A]()
        group
      }
    }

  }

}
