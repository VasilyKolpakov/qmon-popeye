package popeye.analytics

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import popeye.pipeline.MetricGenerator

import scala.util.{Success, Failure, Try}

object OtdsbOrcConverter {
  def main(args: Array[String]) {

    val path: Path = new Path("/tmp/parquet.test")
    val file = new File("/tmp/parquet.test")
    if (file.exists()) {
      file.delete()
    }
    val allTagValues = Seq(
      "host" -> Seq("yandex.ru", "test.com", "dev.net"),
      "cluster" -> Seq("korgen", "tasmania"),
      "dc" -> Seq("ugr", "fol", "iva")
    )

    var t = 0

    val inspector = ObjectInspectorFactory.getReflectionObjectInspector(classOf[TsdbRowCompressed], ObjectInspectorOptions.JAVA)
    usingOrcWriter(path, inspector) {
      orcWriter =>
        for {
          metric <- MetricGenerator.metrics
          tags <- MetricGenerator.generateTags(allTagValues)
        } {
          val tagsString = tags.map { case (name, value) => f"$name=$value" }.mkString(",")
          t += 1
          if (t == 1) {
            orcWriter.addRow(TsdbRowCompressed(metric, tagsString, t, t * 2))
          }
        }
    }
  }

  def usingOrcWriter[S, R](path: Path, inspector: ObjectInspector)(operation: orc.Writer => R) = {
    val writer = createOrcWriter(path, inspector)
    try {
      operation(writer)
    } finally {
      writer.close()
    }
  }

  def usingOrcWriters[A](pathAndClasses: Seq[(Path, ObjectInspector)])(operation: Seq[orc.Writer] => A) = {
    val writerTrys = pathAndClasses.map { case (path, inspector) => Try(createOrcWriter(path, inspector)) }
    val failedCreation = writerTrys.collect { case Failure(t) => t }.headOption
    for (fail <- failedCreation) {
      writerTrys.collect { case Success(writer) => writer }.foreach(_.close())
      throw fail
    }
    val writers = writerTrys.map(_.get)
    try {
      operation(writers)
    } finally {
      writers.foreach(_.close())
    }
  }

  def createOrcWriter[A](path: Path, inspector: ObjectInspector) = {
    val writerOptions = OrcFile.writerOptions(new Configuration())
    writerOptions.inspector(inspector)
    OrcFile.createWriter(path, writerOptions)
  }
}

case class TsdbRowCompressed(metric: String, tags: String, baseTime: Int, points: Int)
