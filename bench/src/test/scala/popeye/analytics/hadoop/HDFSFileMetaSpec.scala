package popeye.analytics.hadoop

import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, FlatSpec}

class HDFSFileMetaSpec extends FlatSpec with Matchers {
  behavior of "HDFSFileMeta serialization"

  it should "pass simple roundtrip" in {
    roundtrip(HDFSFileMeta(new Path("/user/quasi/korgen"), 1000, Vector("host1.yandex.net", "host2.yandex.net")))
  }

  it should "tolerate dots in path" in {
    roundtrip(HDFSFileMeta(new Path("/user/quasi/korgen/some.file"), 1000, Vector("host1.yandex.net", "host2.yandex.net")))
  }

  it should "handle 'no locations' case" in {
    roundtrip(HDFSFileMeta(new Path("/user/quasi/korgen/some.file"), 1000, Vector()))
  }

  it should "serialize sequences" in {
    roundtripSeq(Seq())
    roundtripSeq(Seq(
      HDFSFileMeta(new Path("/user/quasi/korgen"), 1000, Vector("host1.yandex.net", "host2.yandex.net")),
      HDFSFileMeta(new Path("/user/quasi/korgen/some.file"), 1000, Vector("host1.yandex.net", "host2.yandex.net")),
      HDFSFileMeta(new Path("/user/quasi/korgen/some.file"), 1000, Vector())
    ))
  }


  def roundtrip(split: HDFSFileMeta) = {
    val string = HDFSFileMeta.serializeToString(split)
    HDFSFileMeta.deserializeFromString(string) should equal(split)
  }

  def roundtripSeq(splits: Seq[HDFSFileMeta]) = {
    val string = HDFSFileMeta.serializeSeqToString(splits)
    HDFSFileMeta.deserializeSeqFromString(string) should equal(splits)
  }
}
