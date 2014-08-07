package popeye.analytics.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.hive.ql.io.orc

object OrcFileOutputFormat {
  val structClassNameKey = "popeye.analytics.hadoop.OrcFileOutputFormat.structClassNameKey"
}

class OrcFileOutputFormat[A] extends OutputFormat[NullWritable, A] {

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[NullWritable, A] = {
    val configuration = context.getConfiguration
    val structClass = configuration.getClass(OrcFileOutputFormat.structClassNameKey, classOf[NullWritable])
    val writerOptions = OrcFile.writerOptions(configuration)
    val inspector = ObjectInspectorFactory.getReflectionObjectInspector(structClass, ObjectInspectorOptions.JAVA)
    writerOptions.inspector(inspector)
//    context.get
//    OrcFile.createWriter(path, writerOptions)
//
    ???
  }

  override def checkOutputSpecs(context: JobContext): Unit = ???

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = ???
}

class OrcFileRecordWriter[A](orcWriter: orc.Writer) extends RecordWriter[NullWritable, A] {


  override def write(key: NullWritable, value: A): Unit = {
    orcWriter.addRow(value)
  }

  override def close(context: TaskAttemptContext): Unit = {
    orcWriter.close()
  }
}
