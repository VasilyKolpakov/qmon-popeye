package popeye.query

import org.scalatest.FlatSpec
import popeye.Point
import popeye.query.PointSeriesUtils.Line
import popeye.test.PopeyeTestUtils.time
import scala.util.Random
import org.scalatest.Matchers

class PointSeriesUtilsSpec extends FlatSpec with Matchers {

  behavior of "PointSeriesUtils.toLines"

  it should "work" in {
    val input = (0 to 5).map(n => Point(n, 0.0))
    val lines = PointSeriesUtils.toLines(input.iterator).toList
    def line(x1: Int, x2: Int) = Line(x1, 0.0, x2, 0.0)
    val expectedLines = List(
      line(0, 1),
      line(1, 2),
      line(2, 3),
      line(3, 4),
      line(4, 5)
    )
    lines should equal(expectedLines)
  }

  behavior of "PointSeriesUtils.interpolateAndDownsample"

  it should "not output duplicate points" in {
    val input = Seq(
      Seq(Point(0, 0), Point(10, 0)),
      Seq(Point(0, 0), Point(10, 0)),
      Seq(Point(0, 0), Point(10, 0))
    )
    val result = PointSeriesUtils.interpolateAndDownsample(input.map(_.iterator), seq => seq.max, Some(10)).toList
    result should equal(Seq(Point(5, 0)))
  }

  it should "behave as no-op on duplicated input" in {
    val input = (1 to 5).map(_ => (1 to 5).map(n => Point(n, n.toDouble)))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(5)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointSeriesUtils.interpolateAndDownsample(input.map(_.iterator), aggregator, Some(1))
    out.toList should equal(input(0).toList)
  }

  it should "handle lagged series" in {
    val input = Seq(
      Seq(Point(20, 0), Point(30, 0)),
      Seq(Point(0, 0), Point(10, 0))
    )
    val out = PointSeriesUtils.interpolateAndDownsample(input.map(_.iterator), seq => seq.max, Some(10))
    out.toList should equal(Seq(Point(5, 0)))
  }

  it should "pass randomized test" in {
    val random = new Random(0)
    for (_ <- 1 to 100) {
      val dsInterval = random.nextInt(200) + 10
      val numberOfInputs = 20
      val inputs = List.fill(numberOfInputs)(randomInput(random))
      val xs = {
        val minX = inputs.map(_.head.timestamp).min
        val start = minX - minX % dsInterval + dsInterval / 2
        start to Int.MaxValue by dsInterval
      }
      val out = PointSeriesUtils.interpolateAndDownsample(inputs.map(_.iterator), maxAggregator, Some(dsInterval)).toList
      val expectedOut = slowInterpolation(inputs, maxAggregator, xs)
      if (out != expectedOut) {
        println(inputs)
        println(out)
        println(expectedOut)
      }
      out should equal(expectedOut)
    }
  }

  behavior of "PointSeriesUtils.interpolateAndAggregate"

  it should "behave as no-op on single input" in {
    val input = (1 to 5).map(n => Point(n, n.toDouble))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(1)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointSeriesUtils.interpolateAndAggregate(Seq(input.iterator), aggregator)
    out.toList should equal(input.toList)
  }

  it should "behave as no-op on duplicated input" in {
    val input = (1 to 5).map(_ => (1 to 5).map(n => Point(n, n.toDouble)))
    val aggregator: Seq[Double] => Double = {
      seq =>
        seq.size should be(5)
        seq.foldLeft(-1.0)((acc, x) => x)
    }
    val out = PointSeriesUtils.interpolateAndAggregate(input.map(_.iterator), aggregator)
    out.toList should equal(input(0).toList)
  }

  it should "pass randomized test" in {
    val random = new Random(0)
    for (_ <- 1 to 100) {
      val numberOfInputs = 20
      val inputs = List.fill(numberOfInputs)(randomInput(random))
      val out = PointSeriesUtils.interpolateAndAggregate(inputs.map(_.iterator), maxAggregator).toList
      val xs = {
        val allXs =
          for {
            graph <- inputs
            if graph.size > 1
            Point(x, _) <- graph
          } yield x
        allXs.distinct.sorted
      }
      val expectedOut = slowInterpolation(inputs, maxAggregator, xs)
      if (out != expectedOut) {
        println(inputs)
        println(out)
        println(expectedOut)
      }
      out should equal(expectedOut)
    }
  }

  ignore should "have reasonable performance" in {
    def series = (0 to 1000000).iterator.map {
      i => Point(i, i.toDouble)
    }

    val input = (1 to 50).map(_ => series)
    val outputIterator = PointSeriesUtils.interpolateAndAggregate(input, _.sum)
    val workTime = time {
      while(outputIterator.hasNext) {
        outputIterator.next()
      }
    }
    println(f"time in seconds = ${workTime * 0.001}")
  }

  behavior of "PointSeriesUtils.downsample"

  it should "behave as no-op when interval == 1" in {
    val input = (0 to 5).map(i => Point(i, i.toDouble))
    val output = PointSeriesUtils.downsample(input.iterator, 1, maxAggregator).toList
    output should equal(input)
  }

  it should "handle simple case" in {
    val values = (0 until 5).flatMap {
      i =>
        val value = i.toDouble
        Seq.fill(5)(value)
    }
    val timestamps = 0 until 25
    val input = (timestamps zip values).map { case (ts, value) => Point(ts, value)}
    val output = PointSeriesUtils.downsample(input.iterator, 5, maxAggregator).toList
    val expectedOutputValues = (0 until 5).map(i => i.toDouble)
    output.map(_.value) should equal(expectedOutputValues)
  }

  // https://github.com/OpenTSDB/opentsdb/pull/325
  it should "align the start of downsampling" in {
    val input = Seq(Point(100, 1), Point(200, 1), Point(350, 2), Point(500, 2))
    val output = PointSeriesUtils.downsample(input.iterator, 300, maxAggregator).toList
    val expectedOutputValues = Seq(Point(150, 1), Point(450, 2))
    output.toList should equal(expectedOutputValues)
  }

  it should "pass randomized test" in {
    val random = new Random(0)
    val input = randomInput(random)
    val intervalLength = 1000 //1 + random.nextInt(10)
    val output = PointSeriesUtils.downsample(input.iterator, intervalLength, maxAggregator).toList
    val expectedOutput = slowDownsampling(input, intervalLength, maxAggregator)
    if (output != expectedOutput) {
      println(output)
      println(expectedOutput)
    }
    output should equal(expectedOutput)
  }

  ignore should "have reasonable performance" in {
    val series = (0 to 10000000).iterator.map {
      i => Point(i, i.toDouble)
    }

    val outputIterator = PointSeriesUtils.downsample(series, 100, maxAggregator)
    val workTime = time {
      while(outputIterator.hasNext) {
        outputIterator.next()
      }
    }
    println(f"time in seconds = ${workTime * 0.001}")
  }

  behavior of "PointSeriesUtils.differentiate"

  it should "not fail on empty source" in {
    PointSeriesUtils.differentiate(Iterator.empty).hasNext should be(false)
    PointSeriesUtils.differentiate(Iterator(Point(1, 1.0))).hasNext should be(false)
  }

  it should "differentiate constant" in {
    val constPlot = (0 to 10).map(i => Point(i, 1.0))
    val div = PointSeriesUtils.differentiate(constPlot.iterator).toList
    div.map { case Point(x, y) => x} should equal(1 to 10)
    all(div.map { case Point(x, y) => math.abs(y)}) should (be < 0.000001)
  }

  it should "differentiate linear function" in {
    val constPlot = (0 to 10).map(i => Point(i, i.toDouble))
    val div = PointSeriesUtils.differentiate(constPlot.iterator).toList
    div.map { case Point(x, y) => x} should equal(1 to 10)
    all(div.map { case Point(x, y) => y}) should be(1.0 +- 0.000001)
  }

  private def slowDownsampling(source: Seq[Point],
                               intervalLength: Int,
                               aggregator: Seq[Double] => Double): List[Point] = {
    def intervalStartTime(timestamp: Int) = timestamp - timestamp % intervalLength
    val groupedByIntervals = source.groupBy(p => intervalStartTime(p.timestamp))
    val aggregations = groupedByIntervals.map {
      case (intervalStartTime, points) =>
        val intervalTs = intervalStartTime + intervalLength / 2
        val intervalValue = aggregator(points.map(_.value))
        Point(intervalTs, intervalValue)
    }
    aggregations.toList.sortBy(_.timestamp)
  }

  private def maxAggregator(seq: Seq[Double]): Double = seq.max

  private def avgAggregator(seq: Seq[Double]): Double = seq.sum / seq.size

  private def randomInput(random: Random): Seq[Point] = {
    val inputSize = 2 + random.nextInt(50)
    val xs = {
      val randomDeltas = List.fill(inputSize)(random.nextInt(100) + 1)
      randomDeltas.scan(0)(_ + _)
    }
    val ys = List.fill(inputSize)(random.nextDouble())
    (xs.toList.sorted zip ys).map { case (x, y) => Point(x, y)}
  }

  private def slowInterpolation(graphs: Seq[Seq[Point]], aggregationFunction: Seq[Double] => Double, xs: Seq[Int]) = {
    val points = xs.iterator.map {
      x =>
        val interpolations = graphs.map(interpolate(_, x)).filter(_.isDefined).map(_.get)
        if (interpolations.nonEmpty) {
          Some(Point(x, aggregationFunction(interpolations)))
        } else {
          None
        }
    }
    points.takeWhile(_.isDefined).map(_.get).toList

  }

  private def interpolate(graph: Seq[Point], x: Int): Option[Double] = {
    if (graph.size < 2) return None
    val closestLeftPoint = graph.takeWhile { case Point(graphX, _) => graphX <= x}.lastOption
    val closestRightPoint = graph.dropWhile { case Point(graphX, _) => graphX < x}.headOption
    for {
      Point(x1, y1) <- closestLeftPoint
      Point(x2, y2) <- closestRightPoint
    } yield Line(x1, y1, x2, y2).getY(x)
  }
}
