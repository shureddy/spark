package com.shu.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._

import org.apache.spark.sql.functions._

import org.apache.spark._
import com.sun.org.apache.xalan.internal.xsltc.compiler.ForEach
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd._

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
    val sch_seq = df_udf.schema.fields.toSeq //print all the fields with data types associated with it.
    println(sch_seq)
    val sch_names = df_udf.schema.names.toSeq //print only the field names.
    println(sch_names)
    sch_names.foreach(println)

    /*
     * select statements
     */
    //val cols=Array("id","udf")
    df_udf.select('id).show(false)
    df_udf.selectExpr("int(id) +1 as incr_1", "id", "concat(udf,'+s') as udf", "upper(udf) as upper_udf").show(false)
    val als_df = df_udf.select($"id", df_udf("udf"), df_udf.col("id").alias("als_id"))
    println("select data using list of cols")
    val cols = Seq[String]("id", "als_id").map(n => col(n))
    als_df.select(cols: _*).show(false)
    als_df.select(col("*")).show(false) //to select all the columns
    /*
     * registering a temp table
    */
    als_df.createOrReplaceTempView("spark_tmp")
    spark.sql("select id from spark_tmp").show(false)
    /*
     * create data frames
     */
    val df_k1 = Seq((1, "scala"), (2, "java")).toDF("id", "name")
    df_k1.show(false)
    val df_k2 = spark.createDataFrame(Seq((1, "scala"), (2, "java"))) //gets column names as _1,_2
    val df_k3 = spark.createDataFrame(Seq((1, "scala"), (2, "java"))).toDF("id", "name")
    //create an rdd with type RDD[Row]

    val rdd: RDD[Row] = spark.sparkContext.parallelize(Seq(Row(1, "scala"), Row(2, "java")))

    val sch = new StructType()
      .add(StructField("id", IntegerType, true))
      .add(StructField("name", StringType, true))

    //now create dataframe using row rdd and schema sch
    val df_k4 = spark.createDataFrame(rdd, sch)
    df_k4.show(false)
    df_k4.printSchema()
  }
}
/*
 * object for creating and register as udf.
 */
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
