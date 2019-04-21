package com.shu.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object structured_streaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("str_stream").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val data = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    println("-**-" * 10)
    //============dataframe operations========
    val qry = data.groupBy("value")
                  .agg(count("value")
                  .alias("cnt"))
                  .withColumn("lo", lit(current_timestamp()))
                  .select("*")
                  .writeStream
                  .outputMode("complete")
                  .format("console")
                  .start()
    qry.awaitTermination()
    //===========dataset operations===========
    /*
    val splt = data.as[String].flatMap(_.split(" "))
    val qry1 = splt.groupBy("value").count().writeStream.outputMode("complete").format("console").start()
    qry1.awaitTermination()
    */
  }
}