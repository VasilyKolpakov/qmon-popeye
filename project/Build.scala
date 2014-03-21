import sbt._
import sbt.ExclusionRule
import sbt.Keys._
import QMonDistPlugin._
import net.virtualvoid.sbt.graph.{Plugin => Dep}
import scala._

object Util {
  implicit def dependencyFilterer(deps: Seq[ModuleID]) = new Object {
    def excluding(group: String, artifactId: String) =
      deps.map(_.exclude(group, artifactId))
    def excluding(rules: ExclusionRule*) =
      deps.map(_.excludeAll(rules :_*))
  }
}

object Compiler {
  val defaultSettings = Seq(
    scalacOptions in Compile ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature",
      "-language:postfixOps", "-language:implicitConversions"),
    javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6"),
    resolvers ++= Seq(
      "spray repo" at "http://repo.spray.io",
      Resolver.url("octo47 repo", url("http://octo47.github.com/repo/"))({
        val patt = Resolver.mavenStylePatterns.artifactPatterns
        new Patterns(patt, patt, true)
      })
    ),
    ivyXML :=
      <dependencies>
        <exclude org="log4j"/>
        <exclude org="junit" name="junit"/>
        <exclude org="org.slf4j" name="slf4j-log4j12"/>
      </dependencies>
  )
}

object Tests {

  val defaultSettings = Seq(
    parallelExecution in Test := false
  )
}

object Version {
  val Scala = "2.10.3"
  val Akka = "2.2.3"
  val Spray = "1.2-RC2"
  val ScalaTest = "2.0"
  val Mockito = "1.9.0"
  val Jackson = "1.8.8"
  val Kafka = "0.8.1"
  val Metrics = "3.0.0"
  val Slf4j = "1.7.5"
  val Logback = "1.0.7"
  val Snappy = "1.0.5"
  val Guava = "11.0.2"
  val FakeHBase = "0.1.2"
  val Scopt = "3.1.0"
  val Avro = "1.7.5"

  val slf4jDependencies: Seq[ModuleID] = Seq(
    "org.slf4j" % "jcl-over-slf4j" % Version.Slf4j,
    "org.slf4j" % "log4j-over-slf4j" % Version.Slf4j,
    "ch.qos.logback" % "logback-classic" % Version.Logback
  )

  val commonExclusions = Seq(
    ExclusionRule(name = "jline"),
    ExclusionRule(name = "junit")
  )

  val slf4jExclusions = Seq(
    ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "slf4j-simple")
  )
}

object HBase {

  import Util._

  val Hadoop = "2.0.0-cdh4.3.1"
  val HBase = "0.94.6-cdh4.3.1"

  val settings = Seq(
    resolvers ++= Seq(
      "cdh4.3.1" at "https://repository.cloudera.com/artifactory/cloudera-repos"
    ),
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-common" % Hadoop,
      "org.apache.hbase" % "hbase" % HBase
    ).excluding(
      ExclusionRule(name = "commons-daemon"),
      ExclusionRule(name = "commons-cli"),
      ExclusionRule(name = "commons-logging"),
      ExclusionRule(name = "jsp-api"),
      ExclusionRule(name = "servlet-api"),
      ExclusionRule(name = "kfs"),
      ExclusionRule(name = "avro"),
      ExclusionRule(name = "mockito-all"),
      ExclusionRule(organization = "org.jruby"),
      ExclusionRule(organization = "tomcat"),
      ExclusionRule(organization = "org.apache.thrift"),
      ExclusionRule(organization = "com.jcraft"),
      ExclusionRule(organization = "org.mortbay.jetty"),
      ExclusionRule(organization = "com.sun.jersey")
    ).excluding(Version.slf4jExclusions :_*)
     .excluding(Version.commonExclusions :_*)
  )
}

object PopeyeBuild extends Build {

  import Util._

  lazy val defaultSettings =
    Defaults.defaultSettings ++
      Compiler.defaultSettings ++
      Tests.defaultSettings ++
      Dep.graphSettings

  lazy val popeye = Project(
    id = "popeye",
    base = file("."),
    settings = defaultSettings
  ).aggregate(popeyeCore, popeyeBench)

  lazy val popeyeCore = Project(
    id = "popeye-core",
    base = file("core"),
    settings = defaultSettings ++ QMonDistPlugin.distSettings ++ HBase.settings)
    .settings(
    distMainClass := "popeye.Main",
    libraryDependencies ++= Version.slf4jDependencies ++ Seq(
      "com.github.scopt" %% "scopt" % Version.Scopt,
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "org.apache.kafka" %% "kafka" % Version.Kafka,
      "nl.grons" %% "metrics-scala" % Version.Metrics,
      "org.codehaus.jackson" % "jackson-core-asl" % Version.Jackson,
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "org.xerial.snappy" % "snappy-java" % Version.Snappy,
      "com.google.guava" % "guava" % Version.Guava % "test",
      "io.spray" % "spray-can" % Version.Spray,
      "io.spray" % "spray-io" % Version.Spray,
      "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.4",
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test",
      "org.kiji.testing" %% "fake-hbase" % Version.FakeHBase % "test",
      "org.apache.avro" % "avro" % Version.Avro % "test"
    ).excluding(Version.slf4jExclusions :_*)
     .excluding(Version.commonExclusions :_*)
  )

  lazy val popeyeBench = Project(
    id = "popeye-bench",
    base = file("bench"),
    settings = defaultSettings ++ QMonDistPlugin.distSettings).dependsOn(popeyeCore)
    .settings(
    distMainClass := "popeye.transport.bench.GenerateMain",
    libraryDependencies ++= Version.slf4jDependencies ++ Seq(
      "nl.grons" %% "metrics-scala" % Version.Metrics,
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "org.apache.kafka" %% "kafka" % Version.Kafka,
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
    ).excluding(Version.commonExclusions :_*)
     .excluding(Version.slf4jExclusions :_*)
  )

}

