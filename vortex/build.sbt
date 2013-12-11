name := "vortex"

version := "1.0.0-SNAPSHOT"

organization := "edu.jhu.rebar"

libraryDependencies ++= Seq(
  "edu.jhu.rebar" %% "rebar-config" % "1.0.0-SNAPSHOT",
  "edu.jhu.hlt.rebar" % "rebar" % "2.0.0-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % "2.2.1",
  "com.typesafe.akka" %% "akka-testkit" % "2.2.1",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "edu.jhu.hlt.concrete" % "concrete-core" % "1.0.0-SNAPSHOT",
  "org.apache.hadoop" % "hadoop-core" % "1.1.1",
  "org.apache.accumulo" % "accumulo-core" % "1.5.0",
  "org.apache.accumulo" % "accumulo-server" % "1.5.0",
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" exclude ("org.slf4j", "slf4j-log4j12"),
  "log4j" % "log4j" % "1.2.15"
  ).map(_.exclude ("javax.jms", "jms"))
   .map(_.exclude ("com.sun.jmx", "jmxri"))
   .map(_.exclude ("com.sun.jdmk", "jmxtools"))

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
