package com.teamone.spark


import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LDAModel, LocalLDAModel, OnlineLDAOptimizer}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.languageFeature.implicitConversions

object LdaExample2 extends App {
  val sparkSession = SparkSession.builder()
    .appName("LDA topic modeling")
    .master("local[*]").getOrCreate()

  val df: DataFrame = sparkSession.read.format("csv")
      .option("header","true")
      .load("data/actualdata/Tweets.csv")

  val processeddata = Preprocess.run(df,sparkSession)
  val lda_countVector = processeddata._1
//  lda_countVector.take(1)

  val lda = new LDA()
    .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
    .setOptimizer("em")
    .setK(1)
    .setMaxIterations(1)
    .setDocConcentration(-1) // use default values
    .setTopicConcentration(-1)// use default values
//
  val ldaModel: LDAModel = lda.run(lda_countVector)
//
//  val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
//  val vocabList = processeddata._2.vocabulary
//  val topics = topicIndices.map { case (terms, termWeights) =>
//    terms.map(vocabList(_)).zip(termWeights)
//  }
////  println(s"$numTopics topics:")
//  topics.zipWithIndex.foreach { case (topic, i) =>
//    println(s"TOPIC $i")
//    topic.foreach { case (term, weight) => println(s"$term\t$weight") }
//    println(s"==========")
//  }

  LDApridict.run(sparkSession,ldaModel)

}
