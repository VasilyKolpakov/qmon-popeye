package popeye.analytics.hadoop

import org.apache.hadoop.mapreduce._

class OrcFileOutputFormat extends OutputFormat{
  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[Nothing, Nothing] = ???

  override def checkOutputSpecs(context: JobContext): Unit = ???

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = ???
}
