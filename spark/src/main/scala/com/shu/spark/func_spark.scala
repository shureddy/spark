package com.shu.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._

import org.apache.spark.sql.functions._

import org.apache.spark._
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
    val df_k1 = Seq((1, 11, "scala"), (2, 22, "java")).toDF("id", "id_1", "name")
    df_k1.show(false)
    df_k1.select(df_k1.columns.filter(_.startsWith("id")).map(df_k1(_)): _*).show(false) //select cols starts with id

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

    /*
     *check the columns in df and add if the columns not presented.
     */

    val dd_1 = spark.range(10)
    dd_1.show()
    val req_col = Seq("c1", "c2", "c3").filterNot(dd_1.schema.fieldNames.contains).map(lit(0).as(_)) //filter not is inverse of filter and return all the columns are not present in df_1.
    dd_1.select($"*" +: req_col: _*).show()

    val emp_df = spark.emptyDataFrame //create an empty dataframe
    emp_df.show()
    val req_cols = Seq("c1", "c2", "c3")
    /*
     * check the columns presented in the df or add new columns with default values.
     */
    val df_req = req_cols.foldLeft(df) {
      case (d, c) =>
        if (d.columns.contains(c)) {
          d
        } else {
          d.withColumn(c, lit("i'm new"))
        }
    }
    df_req.show(false)

    /*
     * filter in an array
     */

    val df_ar_fil = spark.sparkContext.parallelize(Seq((("ID1", "A", 1)), (("ID1", "B", 5)), (("ID2", "A", 12)),
      (("ID3", "A", 3)), (("ID3", "B", 3)), (("ID3", "C", 5)), (("ID4", "A", 10))), 2).toDF("ID", "Type", "Value")
    df_ar_fil.show(false)
    df_ar_fil.groupBy('ID).agg(collect_list('Type).as("Types"))
      .select('ID, 'Types).where((size('Types) === 1).and(array_contains('Types, "A"))).show(false)

    df_ar_fil.createOrReplaceTempView("tmp_ar")
    spark.sql("select * from tmp_ar").show(false)
    spark.sql("""select a.ID, a.Type,a.Value from tmp_ar as a, 
                  (select ID, count(*) as cnt_val from tmp_ar group by ID) b 
                  where a.ID = b.ID and (a.Type=="A" and b.cnt_val ==1)""").show(false)

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
