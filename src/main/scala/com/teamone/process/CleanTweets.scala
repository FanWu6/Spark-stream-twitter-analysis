package com.teamone.process

import org.apache.spark.sql._

object CleanTweets {

  def clean(input: String): String = input.substring(input.indexOf(": ") + 2)

}
