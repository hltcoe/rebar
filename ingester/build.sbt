name := "Rebar Akka Ingester"

version := "1.0.0-SNAPSHOT"

organization := "edu.jhu"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.1",
  "com.typesafe.akka" %% "akka-testkit" % "2.2.1",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "edu.jhu.hlt.concrete" % "concrete-core" % "1.0.0-SNAPSHOT"
)

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
