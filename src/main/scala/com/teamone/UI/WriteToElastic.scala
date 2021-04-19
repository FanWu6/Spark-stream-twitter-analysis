package com.teamone.UI

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


import java.text.SimpleDateFormat

object WriteToElastic {

  def main(args: Array[String]): Unit = {

    /*Logger.getLogger("org").setLevel(Level.ERROR)*/

    /*val conf = new SparkConf().setAppName("WtE").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(
      Seq(numbers, airports)
    ).saveToEs("spark/docs")*/

    val sc = SparkSession.builder().appName("WriteToElastic").master("local[*]").getOrCreate()


    val lines: DataFrame = sc.read.option("header", "true").csv("data/actualdata/ElasticTest.csv")

    lines.show()


    // Write DF to elastic search
    lines.write
      .format("org.elasticsearch.spark.sql")
      .option("es.port", 9200)
      .option("es.nodes", "localhost")
      .mode("append")
      .save("tweetsairline/doc")
  }

}
