package com.shu.spark
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object streaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("st")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999)
    val wrd = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    println("-**-" * 10)
    wrd.print()

    //=========Convert RDD to Dataframe and print out the data==========

    wrd.foreachRDD { rdd =>
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      import spark.implicits._
      val df = rdd.toDF("word", "count")
      println("hi")
      df.show(false)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}