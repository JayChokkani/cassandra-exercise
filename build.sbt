
//DataStax Interview

organization := "kduraj"

name := "interview"

version := "1.0.1"

scalaVersion := "2.10.3"

scalacOptions += "-deprecation"


// Code Formatting

scalariformSettings


libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.10" % "test",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.6"
)


