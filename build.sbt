name := "Spark-stream-twitter-analysis"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.0.0",
//  "org.apache.spark" %% "spark-sql" % "3.0.0",
//  "org.apache.spark" %% "spark-mllib" % "3.0.0",
//  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "0.10.2.2",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
//  "org.apache.kafka" %% "kafka" % "0.10.2.2",
//  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",

  "com.typesafe" % "config" % "1.3.3",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
)