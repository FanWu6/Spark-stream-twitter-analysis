package com.teamone.sentiment
import com.teamone.sentiment.SentimentAnalysis
import com.teamone.sentiment.CleanTweets
import com.teamone.sentiment.Sentiment.Sentiment
import org.apache.spark.sql._

object NLPExample extends App{


  val texts: List[String] = List("I am good", "I am sad")

  val outs = SentimentAnalysis.twAndSentiments(texts)

  val outRows = Row.fromSeq(outs)

  println(outRows)



  val row = Row("RT @abcd: I am good")

  val out: (String, Sentiment) = SentimentAnalysis.twAndSentiment(CleanTweets.clean(row.toString))

  val sen: Sentiment = out._2
  print(sen.toString)
}
