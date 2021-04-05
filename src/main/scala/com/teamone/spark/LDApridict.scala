package com.teamone.spark

import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable
import org.apache.spark.sql.{DataFrame, SparkSession}

object LDApridict extends App {
  def run(sparkSession:SparkSession,ldamodel:LDAModel): Unit ={

    val df: DataFrame = sparkSession.read.format("csv")
      .option("header","true")
      .load("data/actualdata/Tweets.csv")

    val processeddata = Preprocess.run(df,sparkSession)
    val lda_countVector = processeddata._1

//    val sameModel = DistributedLDAModel.load(sparkSession.sparkContext,path)
//    val localLDAModel = ldamodel

    //create test input, convert to term count, and get its topic distribution


      val topicDistributions: RDD[(Long, Vector)] = ldamodel.asInstanceOf[DistributedLDAModel].toLocal.topicDistributions(lda_countVector)
    println("-------------")
      println("first topic distribution:"+topicDistributions.first._2.toArray.mkString(", "))
    topicDistributions.take(10).foreach(println)


  }


}
