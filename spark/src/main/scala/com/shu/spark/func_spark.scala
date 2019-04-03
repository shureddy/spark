package com.shu.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._

import org.apache.spark.sql.functions._

import org.apache.spark._
import com.sun.org.apache.xalan.internal.xsltc.compiler.ForEach

object func_spark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = spark.sparkContext.parallelize(0 to 10).toDF("id")
    df.show(false)
    val ag_ud = udf((age: Int) => shared.ag_case(age))
    val df_udf = df.withColumn("udf", ag_ud('id))
    df_udf.show(10, false)
    /*
     * print schema
     */

    println("Schema in different patterns")
    df_udf.printSchema()
    println(df_udf.schema.json)
    val sch_seq=df_udf.schema.fields.toSeq //print all the fields with data types associated with it.
    println(sch_seq)
    val sch_names=df_udf.schema.names.toSeq //print only the field names.
    println(sch_names)
    sch_names.foreach(println)
  }
}

object shared {
  def ag_case(age: Int): String = {
    if (age >= 3)
      return "y"
    else if (age == 1)
      return "one"
    else
      return "nothing"
  }
}
