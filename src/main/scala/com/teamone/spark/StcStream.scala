package com.teamone.spark

import com.teamone.Utils.Configure
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object StcStream extends App {

  val spark = SparkSession.builder
    .master(Configure.sparkc.getString("MASTER_URL"))
    .appName("TweetStream")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  import spark.implicits._

  val ds = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "TwitterData2") //
    .load()

  val selectds = ds.selectExpr("CAST(value AS STRING)")

  val customwriter = new ForeachWriter[Row] {
    def open(partitionId: Long, version: Long): Boolean = {
      true
    }
    def process(record: Row): Unit = {
      // Write string to connection
//      MongoDBConnection.insert(record(0).toString())
        println(record.schema)
      println("---------------")
       println(record.toString())
      println("------end-------")
    }
    def close(errorOrNull: Throwable): Unit = {
      Unit
    }
  }

  val writedf = selectds.writeStream
    .foreach(customwriter)
    .start()

  writedf.awaitTermination()
}
