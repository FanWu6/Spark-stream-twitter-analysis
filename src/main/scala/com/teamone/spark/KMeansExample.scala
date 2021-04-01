package com.teamone.spark

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.{clustering, linalg}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, ml}


object KMeansExample extends App {

  val conf = new SparkConf().setAppName("TopicModel").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val data: RDD[String] = sc.textFile("data/actualdata/twitter2D.txt");

  val parsed_data: RDD[(linalg.Vector, String)] = data.map(line => {
    val array: Array[String] = line.split(",")
    val values: Array[Double] = Array(array(0).toDouble,array(1).toDouble)
    (Vectors.dense(values),array.last)
  })

  parsed_data.cache()
//
//  // Cluster the data into four classes using KMeans
  val numClusters = 4;
  val numIterations = 20;
//
  val kmodel: KMeansModel = KMeans.train(parsed_data.map(p=>p._1),numClusters,numIterations);
//
  val predictions: Array[(String, Int)] = parsed_data.map(line=>{
    val cluster = kmodel.predict(line._1)
    (line._2,cluster)
  }).sortBy(a=>a._2,true,1).collect()

  predictions.toStream.map(element=> "Tweet " + element._1 + "is in cluster " + element._2).foreach(println)

  sc.stop()
}
