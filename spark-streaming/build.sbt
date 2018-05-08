
name := "sparksql_streaming"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.2.1"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % "2.2.0" % "provided",
  "log4j" % "log4j" % "1.2.17",
  "mysql" % "mysql-connector-java" % "5.1.38"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}