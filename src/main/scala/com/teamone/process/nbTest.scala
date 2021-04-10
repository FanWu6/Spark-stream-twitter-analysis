package com.teamone.process

import com.tsukaby.bayes.classifier.BayesClassifier


object nbTest extends App{

  val bayes = new BayesClassifier[String, String]()


  val com1 = "The seat is soft".split("\\s")
  val com2 = "It feels comfortable".split("\\s")
  val com3 = "It flies smoothly".split("\\s")

  val sp1 = "It flies fast".split("\\s")
  val sp2 = "It's too slow".split("\\s")
  val sp3 = "I cannot endure its speed".split("\\s")

  bayes.learn("comfort", com1)
  bayes.learn("comfort", com2)
  bayes.learn("comfort", com3)
  bayes.learn("speed", sp1)
  bayes.learn("speed", sp2)
  bayes.learn("speed", sp3)

  val unknow1 = "It is just uncomfortable".split("\\s")
  val unknow2 = "The seat files slowly".split("\\s")

  println(bayes.classify(unknow1).map(_.category).getOrElse(""))
  println(bayes.classify(unknow1).map(_.category).getOrElse(""))
}
