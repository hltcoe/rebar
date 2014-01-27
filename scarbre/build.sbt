name := "rebar-core"

version := "1.0.0-SNAPSHOT"

organization := "edu.jhu.hlt"

scalaVersion := "2.10.3"

initialCommands := """
import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar._
import edu.jhu.hlt.rebar.accumulo._
"""

libraryDependencies ++= Seq(
  "edu.jhu.hlt" %% "concrete-scala" % "2.0.3-SNAPSHOT",
  "edu.jhu.hlt" %% "rebar-config" % "1.0.0-SNAPSHOT",
  "com.twitter" %% "scrooge-serializer" % "3.12.1",
  "org.specs2" %% "specs2" % "2.3.7" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.1",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "org.apache.hadoop" % "hadoop-core" % "1.1.1",
  "org.apache.accumulo" % "accumulo-core" % "1.5.0",
  "org.apache.accumulo" % "accumulo-server" % "1.5.0",
  "com.github.nscala-time" %% "nscala-time" % "0.6.0",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" exclude ("org.slf4j", "slf4j-log4j12"),
  "log4j" % "log4j" % "1.2.15"
).map(_.exclude ("javax.jms", "jms"))
   .map(_.exclude ("com.sun.jmx", "jmxri"))
   .map(_.exclude ("com.sun.jdmk", "jmxtools"))

resolvers += "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
