package com.teamone.process
import com.teamone.process.SentimentAnalysis
import com.teamone.process.CleanTweets

import org.apache.spark.sql._

object NLPExample extends App{


  val texts: List[String] = List("I am good", "I am sad")

  val outs = SentimentAnalysis.twAndSentiments(texts)

  val outRows = Row.fromSeq(outs)

  println(outRows)



  val row = Row("RT @abcd: I am good")

  val out = SentimentAnalysis.twAndSentiment(CleanTweets.clean(row.toString))

  println(out)
}
