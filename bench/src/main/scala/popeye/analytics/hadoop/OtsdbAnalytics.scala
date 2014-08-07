package popeye.analytics.hadoop

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat
import org.apache.hadoop.io.{Writable, NullWritable, Text, IntWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{MultipleOutputs, FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.{ToolRunner, Tool}
import popeye.analytics.hadoop.JavaOtsdbAnalytics.BaseTimeAndDeltasWritable

object OtsdbAnalytics extends Configured with Tool {

  val inputPathKey = "popeye.inputPath"
  val outputPathKey = "popeye.outputPath"
  val jarPathKey = "popeye.jarsPath"
  val nameColumnPathKey = "popeye.nameColumnPath"

  def main(args: Array[String]) {
    ToolRunner.run(OtsdbAnalytics, args)
  }

  override def run(args: Array[String]): Int = {
    val conf = getConf
    val job = Job.getInstance(conf)

    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Writable])

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[BaseTimeAndDeltasWritable])

    job.setMapperClass(classOf[JavaOtsdbAnalytics.OAMapper])
    job.setReducerClass(classOf[JavaOtsdbAnalytics.OAReducer])

    val inputPath = conf.get(inputPathKey)
    job.setInputFormatClass(classOf[HFileInputFormat])
    val fs = FileSystem.get(conf)
    val inputFilePath = new Path(inputPath)
    val inputFileLength = fs.getFileStatus(inputFilePath).getLen
    val blockLocations = fs.getFileBlockLocations(inputFilePath, 0, inputFileLength).map(_.getHosts.toSeq)
    val bestLocation = calculateBestLocation(blockLocations)
    HFileInputFormat.setInputPaths(job, Seq(HDFSFileMeta(inputFilePath, inputFileLength, Vector(bestLocation))))
    val outputPath = conf.get(outputPathKey)
    val jarPath = conf.get(jarPathKey)
    val nameColumnPath = conf.get(nameColumnPathKey)

    JavaOtsdbAnalytics.prepareJob(job, nameColumnPath)
    val jars = FileSystem.get(conf).listStatus(new Path(jarPath))

    for (jar <- jars) {
      val path = jar.getPath
      job.addFileToClassPath(path)
    }

    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    val success = job.waitForCompletion(true)
    if (success) 0 else 1
  }

  def calculateBestLocation(blockLocations: Seq[Seq[String]]): String = {
    val numberOfBlocksPerLocation = blockLocations.flatMap(locs => locs).groupBy(loc => loc).mapValues(_.size).toList
    val ordering: Ordering[(String, Int)] = Ordering.by { case (_, numBlks) => numBlks }
    val (loc, maxBlocks) = numberOfBlocksPerLocation.max(ordering)
    loc
  }
}
