package com.teamone.producer

//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.twitter.TwitterUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

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
//    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
//
//    // Get rid of log spam (should be called after the context is set up)
//    setupLogging()
//
//    // Create a DStream from Twitter using our streaming context
//    val filter = List("trump")
//    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None,filter)
//
//    val ts = tweets.filter(_.getLang=="en")
//    val statuses = ts.map(statue=>statue.getText)
//
//    statuses.print()
//    val tweets: DStream[Status] = stream.filter { t =>
//      val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
//      tags.contains("#bigdata") && tags.contains("#food")
//    }

//    tweets.foreachRDD((rdd,time)=>
//            rdd.map(t=>{if(t.getLang.equals("en"))
//              println(t.getText)})
////            rdd.map(t=>(System.out.println(t.getText)))
//          )

//    // Now extract the text of each status update into DStreams using map()
//    val statuses: DStream[String] = tweets.filter(tweet => tweet.getLang.equals("en") || tweet.getLang.equals("")).map(status => status.getText)
//
//    tweets.foreachRDD(rdd => {
//      println("\nNew tweets: ==%s".format(rdd.first().getText))
//    })
//    // Blow out each word into a new DStream
//    val tweetwords: DStream[String] = statuses.flatMap(tweetText => tweetText.split(" "))
////
////    // Now eliminate anything that's not a hashtag
//    val hashtags = tweetwords.filter(word => word.startsWith("#"))
////
////    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
//    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
////
////    // Now count them up over a 5 minute window sliding every one second
//    val hashtagCounts: DStream[(String, Int)] = hashtagKeyValues.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1))
//    //  You will often see this written in the following shorthand:
//    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
//
//    // Sort the results by the count values
//    val sortedResults: DStream[(String, Int)] = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
//
//    // Print the top 10
//    sortedResults.print

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
//    ssc.checkpoint("C:/checkpoint/")
//    ssc.start()
//    ssc.awaitTermination()
  }


}
