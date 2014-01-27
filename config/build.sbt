name := "rebar-config"

version := "1.0.1-SNAPSHOT"

organization := "edu.jhu.hlt"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.0.2"
)

resolvers += "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

credentials += Credentials(Path.userHome / ".sbt" / "coe-credentials")

publishTo <<= version { v: String =>
  val artifactory = "http://test4:8081/artifactory/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at artifactory + "libs-snapshot-local")
  else
    Some("releases" at artifactory + "libs-release-local")
}

