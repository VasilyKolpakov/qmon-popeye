package popeye.analytics

import org.scalatest.{Matchers, FlatSpec}
import IteratorUtils._

import scala.collection.mutable.ArrayBuffer

class IteratorUtilsSpec extends FlatSpec with Matchers {
  behavior of "IteratorUtils.mergeSorted"

  it should "produce empty iterator" in {
    val it = mergeSorted[Int](Seq(
      Iterator(),
      Iterator(),
      Iterator()
    ).map(_.buffered))
    it should be(empty)
  }

  it should "produce sorted iterator" in {
    val result = mergeSorted[Int](Seq(
      Iterator(1, 4, 5),
      Iterator(2),
      Iterator(3, 6)
    ).map(_.buffered)).toSeq
    result should equal(Seq(1, 2, 3, 4, 5, 6))
  }

  behavior of "IteratorUtils.uniqByOrdering"

  it should "produce empty iterator" in {
    val it = uniqByOrdering[Int](Iterator())
    it should be(empty)
  }

  it should "produce uniq iterator" in {
    val result = uniqByOrdering[Int](Iterator(1, 2, 2, 3, 3, 4, 5, 5, 5, 5, 5, 5, 6, 7, 6)).toSeq
    result should be(Seq(1, 2, 3, 4, 5, 6, 7, 6))
  }

  it should "produce single item iterator" in {
    val result = uniqByOrdering[Int](Iterator(1)).toSeq
    result should be(Seq(1))
  }

  it should "produce single item iterator once more" in {
    val result = uniqByOrdering[Int](Iterator(1, 1)).toSeq
    result should be(Seq(1))
  }

  behavior of "IteratorUtils.groupByPredicate"

  it should "produce empty iterator" in {
    val result = groupByPredicate[Int](Iterator[Int](), (a, b) => true).toSeq
    result should be(Seq())
  }

  it should "produce single-item groups" in {
    val result = groupByPredicate[Int](Iterator[Int](1, 2, 3, 4, 5), (a, b) => false).toSeq
    result should be(Seq(1, 2, 3, 4, 5).map(i => ArrayBuffer(i)))
  }

  it should "produce groups" in {
    val result = groupByPredicate[Int](Iterator[Int](1, 2, 3, 4), (a, b) => b - a < 2).toSeq
    result should be(Seq(Seq(1, 2), Seq(3, 4)))
  }

}
