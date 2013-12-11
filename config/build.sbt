name := "rebar-config"

version := "1.0.0-SNAPSHOT"

organization := "edu.jhu.rebar"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.0.2"
)

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
