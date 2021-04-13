package com.teamone.producer

import com.teamone.Utils.Configure
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
//import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import com.teamone.sentiment.Sentiment.Sentiment
import com.teamone.sentiment.{CleanTweets, SentimentAnalysis}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Column, DataFrame, ForeachWriter, Row, SparkSession}

object TweetScrapper {
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    import scala.io.Source

    val lines = Source.fromFile("data/actualdata/twitter.txt")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val keywords =  Configure.tweetfiltersc.getString("KEYWORDS").split(",").toSeq
    println(keywords)
    val tweets = TwitterUtils.createStream(ssc, None,keywords)
    // Now extract the text of each status update into DStreams using map()
    val statuses: DStream[String] = tweets.filter(t=>t.getLang()=="en").map(status => status.getText)


    val spark = SparkSession.builder
      .master(Configure.sparkc.getString("MASTER_URL"))
      .appName("TweetStream")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._
    val model = PipelineModel.read.load("data/nbmodel")
    println("载入model")


    statuses.foreachRDD( rdd => {

      rdd.collect().foreach(row => {

//        println(i)
        val out: (String, Sentiment) = SentimentAnalysis.twAndSentiment(CleanTweets.clean(row.toString))
        //2.fit navie bayes training model
        val seq= Seq((out._1,out._2.toString))
        //        println(seq.toString())
        val df: DataFrame = seq.toDF("text","airline_sentiment")
//                df.show()
        val predictions: DataFrame = model.transform(df)
        predictions.show()
        val result: DataFrame = predictions.select($"text",$"prediction",$"airline_sentiment")
//        .write.format("csv")
//          .mode("append")
//          .save("data/csv")
////
//        println("save")
      })
    })

    // Blow out each word into a new DStream
//    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
//
//    // Now eliminate anything that's not a hashtag
//    val hashtags = tweetwords.filter(word => word.startsWith("#"))
//
//    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
//    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
//
//    // Now count them up over a 5 minute window sliding every one second
//    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
//    //  You will often see this written in the following shorthand:
//    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
//
//    // Sort the results by the count values
//    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
//
//    // Print the top 10
//    sortedResults.print

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }


}
