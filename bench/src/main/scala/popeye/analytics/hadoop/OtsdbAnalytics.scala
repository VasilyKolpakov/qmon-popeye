package popeye.analytics.hadoop

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.{ToolRunner, Tool}

object OtsdbAnalytics extends Configured with Tool {

  val inputPathKey = "popeye.inputPath"
  val outputPathKey = "popeye.outputPath"
  val jarPathKey = "popeye.jarsPath"

  def main(args: Array[String]) {
    ToolRunner.run(OtsdbAnalytics, args)
  }

  override def run(args: Array[String]): Int = {
    val conf = getConf
    val job = Job.getInstance(conf)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[JavaOtsdbAnalytics.OAMapper])
    job.setReducerClass(classOf[JavaOtsdbAnalytics.OAReducer])

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    val inputPath = conf.get(inputPathKey)
    val outputPath = conf.get(outputPathKey)
    val jarPath = conf.get(jarPathKey)
    val jars = FileSystem.get(conf).listStatus(new Path(jarPath))

    for (jar <- jars) {
      val path = jar.getPath
      job.addFileToClassPath(path)
    }


    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    val success = job.waitForCompletion(true)
    if (success) 0 else 1
  }
}
