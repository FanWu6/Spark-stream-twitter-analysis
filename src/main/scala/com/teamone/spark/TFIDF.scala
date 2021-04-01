package com.teamone.spark

import com.teamone.Utils.Configure
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TFIDF extends App {
  val sparkSession = SparkSession.builder
    .master(Configure.sparkc.getString("MASTER_URL"))
    .appName("TFIDF")
    .getOrCreate()

  val df = sparkSession.read.csv("data/actualdata/Tweets.csv")


//  val tokenizer = new Tokenizer()
//    .setInputCol("_c10")
//    .setOutputCol("words")
////
//  val words: DataFrame = tokenizer.transform(df)

//  words.select("words").take(5).foreach(println)
//
//  val remover = new StopWordsRemover()
//    .setInputCol("words")
//    .setOutputCol("noStopWords")
//
//  val noStopWords = remover.transform(words)
//
//noStopWords.select("words","noStopWords").take(5).foreach(println)
//
//  val hashingtf = new HashingTF()
//    .setInputCol("words")
//    .setOutputCol("rawFeatures")
//    .setNumFeatures(20)
//
//  val tf = hashingtf.transform(words)
//
//  val idf = new IDF()
//    .setInputCol("rawFeatures")
//    .setOutputCol("features")
//
//  val idfModel = idf.fit(tf)
//
//  val tfidf = idfModel.transform(tf)
//
//  tfidf.select("features")
//
//  tfidf.select("id","features")


  //--------CountVectorizer
//  val cvModel = new CountVectorizer()
//    .setInputCol("words")
//    .setOutputCol("rawFeatures")
//    .setVocabSize(200)
//    .setMinDF(2)
//    .fit(words)
//
//  val countVector = cvModel.transform(words)
//
//  val idf = new IDF()
//    .setInputCol("rawFeatures")
//    .setOutputCol("features")
//
//  val idfModel = idf.fit(countVector)
//
//  val tfcv = idfModel.transform(countVector)
//
//  tfcv.select("features").take(5).foreach(println)
}
