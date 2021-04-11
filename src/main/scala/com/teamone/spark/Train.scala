package com.teamone.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes}
import java.io.InputStream


/**
  * Created by jxy on 2016-06-30.
  */
object Train {
  def main(args: Array[String]): Unit = {
    println("Initializing Spark Context for training machine learning model...")
    val sparkConf = new SparkConf().setAppName("Train Naive Bayes model").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //load stop words 此处读的是resources下的目录
    val stream: InputStream = getClass.getResourceAsStream("/stopwords.txt")
    val lines = scala.io.Source.fromInputStream(stream).getLines.toSet
    val stopWords = sc.broadcast(lines).value


    //load training data set
    println("Loading training data set ...")
    val rawTrainingDataSet = sc.textFile("data/actualdata/train.csv")
    val trainingDataSet = rawTrainingDataSet.map(lines => TrainingUtils.toTuple(lines))
                                            .map(x => (x._1, TrainingUtils.filterStopWords(x._2, stopWords)))
                                            .map(x => (x._1, TrainingUtils.featureVectorization(x._2)))
                                            .map(x => new LabeledPoint((x._1).toDouble, x._2))
//  trainingDataSet.foreach(print)

    //loading testing data set
    println("Loading testing data set ...")
    val rawTestingDataSet = sc.textFile("data/actualdata/nbtest.csv")
    val testingDataSet = rawTestingDataSet.map(lines => TrainingUtils.toTuple(lines))
//      .map(x => (x._1, TrainingUtils.filterStopWords(x._2, stopWords)))
                                         .map(x => (x._1, TrainingUtils.featureVectorization(x._2), x._2))
                                         .map(x =>{
                                           val lp = new LabeledPoint((x._1).toDouble, x._2)
                                           (lp, x._3) //_3 is plain text data
                                         })


    //Begin training model
    println("******** Training *********")
    val model =NaiveBayes.train(trainingDataSet, lambda = 1.0, modelType = "multinomial")
    println("******** Finish Training *******")

    //prediction value vs testing actual label
    val predictionAndLabel = testingDataSet.map(p => {
      val labeledPoint = p._1
      val text = p._2
      val features = labeledPoint.features
      val actualLabel = labeledPoint.label
      val predictedLabel = model.predict(features)
      (actualLabel, predictedLabel, text)
    })

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testingDataSet.count()

    println("Training and Testing complete. Accuracy is = " + accuracy)

    predictionAndLabel.take(10).foreach( x => {
      println("---------------------------------------------------------------")
      println("Text = " + x._3)
      println("Actual Label = " + ( x._1  match {
        case 0 => "Late"
        case 2 => "Bad Fligt"
        case 4 => "Customer Service Issue"
        case 6 => "Good"
      }))
      println("Predicted Label = " + (x._2 match {
        case 0 => "Late"
        case 2 => "Bad Fligt"
        case 4 => "Customer Service Issue"
        case 6 => "Good"
      }))
      println("----------------------------------------------------------------\n\n")
    } )

    //saving model 先不要保存模型
    println("************** Saving Model **************")
    model.save(sc, "src/main/nbmodel/")

    sc.stop()
    print("******** Spark Model Training Done **********")


  }
}
