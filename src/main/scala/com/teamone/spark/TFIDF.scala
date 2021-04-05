package com.teamone.spark

import com.teamone.Utils.Configure
import com.teamone.spark.KMeansExample.data
import com.teamone.spark.StcStream.spark
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TFIDF extends App {
  import spark.implicits._
  val sparkSession = SparkSession.builder
    .master(Configure.sparkc.getString("MASTER_URL"))
    .appName("TFIDF")
    .getOrCreate()
//
//  val df: DataFrame = sparkSession.read.textFile("data/actualdata/Tweets.csv").toDF().select("text")
//
//  df.show(5)


  val sentenceData = sparkSession.createDataFrame(Seq(
    (0, "Hi I heard about Spark"),
    (0, "I wish Java could use case classes"),
    (1, "Logistic regression models are neat")
  )).toDF("label", "sentence")

  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val wordsData = tokenizer.transform(sentenceData)
  val hashingTF = new HashingTF()
    .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
  val featurizedData = hashingTF.transform(wordsData)
  // CountVectorizer也可获取词频向量

  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)
  val rescaledData = idfModel.transform(featurizedData)
  rescaledData.select("features", "label").take(3).foreach(println)

  //  val df = sparkSession.read.csv("data/actualdata/Tweets.csv")

  //--------------

//  val df = parsed_data.toDF()
//
//  val hashingTF = new HashingTF()

//  val tf = hashingTF.transform(parsed_data).cache()
//  val idf = new IDF().fit(tf)
//
//  val tf_idf = idf.transform(tf) //计算TF_IDF词频
//  tf_idf.foreach(println)
  //------------------------

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
