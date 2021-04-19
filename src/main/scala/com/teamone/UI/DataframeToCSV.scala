package com.teamone.UI



import java.io.FileWriter

import org.apache.spark.sql.{Column, DataFrame, ForeachWriter, Row, SparkSession}

object DataframeToCSV {

  def dataframeToCSV(dataFrame: DataFrame, fileName: String): Unit = {
    val ColumnSeparator = ","
    val writer = new FileWriter(fileName, true)
    val seq: Seq[Any] = dataFrame.collect.map(_.toSeq).apply(0)
    try{
      writer.write(s"${seq.map(_.toString).mkString(ColumnSeparator)}\n")
    } finally {
      writer.flush()
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().appName("DFToCSV").master("local[*]").getOrCreate()

    import sc.implicits._


    val seq1 = Seq(("This is df test1","negative",0.0))



    val df1: DataFrame = seq1.toDF("text","sentiment","pre")

    df1.show()


    val fileName = "data/TestCSV.csv"

    dataframeToCSV(df1, fileName)


/*    val seqAfterArray: Array[Seq[Any]] = df1.collect.map(_.toSeq)

    val seqAfter: Seq[Any] = seqAfterArray.apply(0)*/


  }

}
