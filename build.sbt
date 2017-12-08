name := "RecommendationEngine"

version := "8.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq("org.apache.kafka" %% "kafka" % "0.10.1.0",

  "org.apache.kafka" % "kafka-clients" % "0.10.2.0",

 "org.apache.spark" % "spark-core_2.11" % "2.1.0",

 "org.apache.spark" % "spark-sql_2.11" % "2.1.0",

"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.0",

 "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0",

 "org.apache.spark" % "spark-streaming_2.11" % "2.1.0",

  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.5",

  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0" % "compile"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


