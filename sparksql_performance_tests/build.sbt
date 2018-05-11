
name := "sparksql_performance_tests"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "log4j" % "log4j" % "1.2.17",
  "mysql" % "mysql-connector-java" % "5.1.38",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test,
  "junit" % "junit" % "4.12" % Test
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}