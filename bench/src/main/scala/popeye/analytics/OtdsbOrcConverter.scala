package popeye.analytics

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import popeye.pipeline.MetricGenerator

object OtdsbOrcConverter {
  def main(args: Array[String]) {

    val path: Path = new Path("/tmp/parquet.test")
    val file = new File("/tmp/parquet.test")
    if (file.exists()) {
      file.delete()
    }
    val writerOptions = OrcFile.writerOptions(new Configuration())
    val inspector = ObjectInspectorFactory.getReflectionObjectInspector(classOf[TsdbRowCompressed], ObjectInspectorOptions.JAVA)
    writerOptions.inspector(inspector)
    val orcWriter = OrcFile.createWriter(path, writerOptions)
    val allTagValues = Seq(
      "host" -> Seq("yandex.ru", "test.com", "dev.net"),
      "cluster" -> Seq("korgen", "tasmania"),
      "dc" -> Seq("ugr", "fol", "iva")
    )

    var t = 0
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
    orcWriter.close()
  }
}

case class TsdbRowCompressed(metric: String, tags: String, baseTime: Int, points: Int)
