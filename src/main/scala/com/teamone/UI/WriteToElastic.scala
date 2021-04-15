package com.teamone.UI

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.text.SimpleDateFormat

object WriteToElastic {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkSession.builder().appName("WriteToElastic").master("local[*]").getOrCreate()


    val lines: DataFrame = sc.read.option("header", "true").csv("data/actualdata/ElasticTest.csv")

    lines.show()


    // Write DF to elastic search
    lines.write
      .format("org.elastic.spark.sql")
      .option("es.port", 9200)
      .option("es.host", "localhost")
      .mode("append")
      .save("usedcarspark/doc")

  }

}
