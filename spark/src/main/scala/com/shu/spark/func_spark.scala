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

    df_ar_fil.createOrReplaceTempView("tmp_ar")
    spark.sql("select * from tmp_ar").show(false)
    spark.sql("""select a.ID, a.Type,a.Value from tmp_ar as a, 
                  (select ID, count(*) as cnt_val from tmp_ar group by ID) b 
                  where a.ID = b.ID and (a.Type=="A" and b.cnt_val ==1)""").show(false)
    /*
     * swap the schemas of df's
     */
    val df_ar_fil2 = spark.sparkContext.parallelize(Seq(("ID3", "A", 2), ("ID3", "C", 4)), 2).toDF("ID1", "Type1", "va1")
    df_ar_fil2.show()

    //to df_ar_fil2 dataframe add df_ar_fil schema to it using spark.createdataframe function.
    spark.time(spark.createDataFrame(df_ar_fil2.rdd, df_ar_fil.schema).show(false)) //caluculate time taken for this action to perform

    /*
     * (or) to get same output as above we need to join to df_ar_fil on
     * id,types(remove []brackets) and select id,type,value
     */
    df_ar_fil.groupBy('ID).agg(collect_list('Type).as("Types"))
      .select('ID, 'Types).where((size('Types) === 1).and(array_contains('Types, "A"))).show(false)
    println("**-" * 100)

    /*
           * date and timestamp functions
           */
    val df_dt = spark.sparkContext
      .parallelize(Seq(("1", "scala"), ("1", "java"), ("1", "python")))
      .toDF("id", "lang")
    //df_dt.show(false)
    df_dt.withColumn("today_date", lit(current_date)) //today date
      .withColumn("sub_year", lit(add_months(current_date, -12)).cast(DateType)) //substract year from today and cast to date type
      .withColumn("time_now", lit(current_timestamp).cast(TimestampType)) //current timestamp in yyyy-MM-dd hh:mm:ss.SSS
      .withColumn("epoch time", lit(unix_timestamp(current_timestamp()))) //get epoch time
      .withColumn("format_date", lit(date_format(current_timestamp(), "yyyy/MM/dd HH/mm/ss/SSS")))
      .withColumn("Year", lit(year(current_timestamp())))
      .withColumn("month", lit(month(current_timestamp())))
      .withColumn("date", lit(to_date(current_timestamp())))
      .withColumn("day", lit(dayofmonth(current_timestamp())))
      .withColumn("yesterday", lit(date_sub(current_date, 1)))
      .withColumn("tomorrow", lit(date_add(current_date, 1)))
      .withColumn("date_difference", datediff(current_timestamp, lit("2018-04-11 21:56:45.882").cast(TimestampType)))
      .withColumn("timestamp_difference", unix_timestamp(current_timestamp) - unix_timestamp(lit("2018-04-11 21:56:45.882").cast(TimestampType)))
      .show(false)
    /*
     * Array operations
     */
    val df_arr = Seq(
      (1, Array("Hi there", "Hello there")),
      (2, Array("Bye now")),
      (3, Array("Thank you", "Thanks", "Many thanks")))
      .toDF("id", "sentences")
    df_arr.show(false)
    println("^^" * 30)
    /*
     * check if array contains specific value
     */
    df_arr.filter(array_contains('sentences, "Thanks")).selectExpr("*").show(false)
    println("^^" * 30)
    /*
     * posexplode is with position column added to the explode
     */
    val df_pos_exp = df_arr.select($"id", posexplode('sentences).as("Position" :: "Column" :: Nil))
      .select(concat('id, lit(":"), 'Position) as "id", $"Column" as "sentences")
    df_pos_exp.show(false)
    /*
     * explode dont add the position column
     */
    val df_exp = df_arr.select('id, explode('sentences))
    df_exp.show(false)
    /*
     * absolute value
     */
    val df_func = spark.sparkContext.parallelize(
      Seq(
        (-10, "we", List("iii", "lop", "lop", "lop")),
        (20, "he", List("iii", "lop", "lop", "lop")),
        (20, "he", List("iii", "lop", "lop", "lop")),
        (20, "he", List("iii", "lop", "lop", "lap")))).toDF("id", "name", "arr")
    /*
		 * print records in vertical..
 		*/
    df_func.withColumn("abs", abs('id))
      .groupBy('id)
      .agg(countDistinct('name).as("cd"), count('name).as('c))
      .select('id, 'cd, 'c,
        when('c <= 3, "less than 3").otherwise("greater than 3").alias("case_stmt"))
      .show(10, 1000, true)
    df_func.withColumn("abs", abs('id))
      .groupBy('id)
      .agg(countDistinct('name).as("cd"), count('name).as('c))
      .select('id, 'cd, 'c,
        when('c <= 3, "less than 3").otherwise("greater than 3").alias("case_stmt"))
      .show(10, true) //print in table format
    /*
     * explain the logical plan.
     */
    df_func.explain()
    df_func.explain(true) //true for extended plan
    df_func.withColumn("leng", length(df_func("name"))).show(false) //find the length of the column
    //df_func.select('arr).distinct().show(false) //find the distinct values
    df_func.select("*").distinct().show(false)
    df_func.show(false)
    df_func.createOrReplaceTempView("tt")
    spark.sql("select distinct * from tt").show(false)
    /*
     * collect_list(have duplicates) and collect_set(no duplicates)
     */
    df_func.groupBy('id)
      .agg(collect_list('name).as('cl_name), collect_set('name).as('cs_name))
      .select('id, 'cl_name, 'cs_name).show(false)

    //===========create map============
    val df_m = Seq(("Texas", "Usa"), ("Andhra", "India")).toDF("state", "country")
    df_m.show(false)
    val df_map = df_m.map { df => (df.getString(0), df.getString(1)) }.collect().toMap
    df_map.foreach(println)
    println(df_map)

    //===========using regular expression extract=============
    val df_reg = Seq(("""192.167.56.1-45195 " GET \docsodb.sp.ip \..\"""")).toDF("value")
    df_reg.show(false)
    val re = """(.*)-(.*?)\s+"\s+(\w+)\s+\\(.*?)\s+\\""" //matching regex to extract capture groups
    df_reg.select(
      regexp_extract('value, re, 1).alias("ip_addr"),
      regexp_extract('value, re, 2).alias("port"),
      regexp_extract('value, re, 3).alias("method"),
      regexp_extract('value, re, 4).alias("desc"))
      .show(false)

    //==========filter using and or operators==========
    val dd = Seq(("1", "SCL"), ("2", "Hv"), ("3", "scl"), ("4", "scl")).toDF("id", "nam")
    dd.show(false)
    dd.filter(('id > 1) && ('id < 4)).select("*").show(false)
    dd.filter(('id === 1) || ('id === 2)).select("*").show(false)
    
    //-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    /*
     *UDF
     */
    def lower(nam: String): String = {
      nam.toLowerCase()
    }

    def cnct(id: String, nam: String): String = {
      id.toString() + nam
    }
    
    //===register udf to use in dataframe api
    val reg_udf = udf[String, String](lower)
    val cnct_udf = udf[String, String, String](cnct)
    
    dd.withColumn("lower", reg_udf('nam))
      .withColumn("cnct", cnct_udf('id, 'nam))
      .select("*")
      .show(false)
    println("--*--" * 10)

    //register udf to use in sparksql
    spark.sqlContext.udf.register("udf_dd", lower _)
    dd.createOrReplaceTempView("tmp")
    spark.sql("select id,nam,udf_dd(nam) lower_case from tmp").show(false)
    
    //-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

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
