package popeye.analytics

import org.apache.hadoop.conf.Configuration
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.hfile.{HFile, CacheConfig}
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.BytesKey

object HFileRanges {

  case class KeyRange(startRowKey: BytesKey, stopRowKey: BytesKey) {
    def isInRange(key: BytesKey) = startRowKey.compareTo(key) <= 0 && key.compareTo(stopRowKey) <= 0

    def isPrefixInRange(prefix: BytesKey) = isInRange(prefix) || {
      val prefixLength = prefix.bytes.length
      if (startRowKey.bytes.length < prefixLength) {
        false
      } else {
        Bytes.equals(prefix.bytes, 0, prefixLength, startRowKey.bytes, 0, prefixLength)
      }
    }
  }


  def hadoopConfiguration(hadoopConfigurationPaths: Seq[String]) = {
    val conf = new Configuration()
    for (path <- hadoopConfigurationPaths) {
      conf.addResource(new File(path).toURI.toURL)
    }
    conf
  }

  def main(args: Array[String]) {
    val conf = hadoopConfiguration(Seq("/etc/hadoop/conf/hdfs-site.xml", "/etc/hadoop/conf/core-site.xml"))
    val hdfs: FileSystem = FileSystem.get(conf)
    val cc = new CacheConfig(conf)
    val hFilesAndKeyRanges = getAllKeyRanges(hdfs, cc, new Path("/user/quasi/korgen/tsdb"))
    hFilesAndKeyRanges.foreach(println)
    hdfs.close()
  }

  def getAllKeyRanges(hdfs: FileSystem, cc: CacheConfig, tsdbPath: Path): Seq[(Path, KeyRange)] = {
    val regionFiles = hdfs.listStatus(tsdbPath).toList.map(_.getPath).filterNot(_.getName.startsWith("."))
    val tFolders = regionFiles.map(p => p.suffix("/t"))
    val hFiles = tFolders.flatMap(f => hdfs.listStatus(f).toList.map(_.getPath)).filterNot(_.getName.contains("."))
    hFiles.map {
      hFile => (hFile, getKeyRange(hFile, hdfs, cc))
    }
  }

  def getKeyRange(hFilePath: Path, fs: FileSystem, cc: CacheConfig) = {
    val reader = HFile.createReader(fs, hFilePath, cc)
    KeyRange(
      startRowKey = new BytesKey(reader.getFirstRowKey),
      stopRowKey = new BytesKey(reader.getLastRowKey)
    )
  }
}
