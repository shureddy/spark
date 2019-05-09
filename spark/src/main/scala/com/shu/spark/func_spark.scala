package com.shu.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{ FileSystem, Path }

object func_spark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    try {
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

      val df_op = spark.sparkContext.parallelize(Seq(
        ("a", 1, 2, 3),
        ("b", 4, 6, 5)))
        .toDF("value", "id1", "id2", "id3")

      val ref_op = spark.sparkContext.parallelize(Seq(
        (1, "apple", "fruit"),
        (2, "banana", "fruit"),
        (3, "cat", "animal"),
        (4, "dog", "animal"),
        (5, "elephant", "animal"),
        (6, "Flight", "object")))
        .toDF("id", "descr", "parent")

      val dfNew = df_op.withColumn("id", explode(array("id1", "id2", "id3")))
        .select("id", "value")

      ref_op.join(dfNew, Seq("id"))
        .groupBy("value")
        .agg(
          concat_ws("+", collect_list("descr")) as "desc",
          concat_ws("+", collect_list("parent")) as "parent")
        .drop("value")
        .show()

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

      /*
       * join and cross joins
       */

      val df_cj, df_cj1 = Seq(
        ("HL_13203", "DELIVERED", 3226),
        ("HL_13203", "UNSEEN", 249),
        ("HL_13203", "UNDELIVERED", 210),
        ("HL_13203", "ASSIGNED", 2),
        ("HL_14108", "DELIVERED", 3083),
        ("HL_14108", "UNDELIVERED", 164),
        ("HL_14108", "PICKED", 1))
        .toDF("code", "status", "count")

      //======alias the dataframe and use alias name in dataframe
      df_cj.alias("f")
        .join(df_cj1.alias("s"), ($"f.code" === $"s.code" && $"f.status" === $"s.status"))
        .show()

      val df_sc = df_cj.select('status.as("st_c")).distinct()
      val df_co = df_cj.select('code.as("c")).distinct()

      //====cross join to get all status to code
      val crs_df = df_sc.crossJoin(df_co)

      //=====left join and fill null with 0
      crs_df.alias("f_d")
        .join(df_cj.alias("s_d"), ($"f_d.c" === $"s_d.code" && $"f_d.st_c" === $"s_d.status"), "left")
        .select("st_c", "c", "count").na.fill(0)
        .show(false)

      /*
       * replace empty whitespaces with null
       */
      val df_ws = Seq((" "), ("1"), ("3")).toDF("id")
      df_ws.show(false)

      //====case stament to check if the id trim+length 0 (or) id =1 then replace with null otherwise id
      df_ws.withColumn("id", when((length(trim('id)) === 0 || 'id === 1), lit(null))
        .otherwise('id))
        .select("*")
        .filter('id.isNull)
        .show(false)

      println("98x" * 10)
      df_ws.select(regexp_replace('id, "^\\s+$", "NA")).show(false)
      //df_ws.selectExpr("id","regexp_replace(id, '^\\s+$','NA')").show(false)
      df_ws.createOrReplaceTempView("df")
      spark.sql("select id,case when length(trim(id)) == 0 then 'NA' else id end,regexp_replace(id,'^(\\s+)$','NA') id2 from df").show(false)
      //========check length of column
      df_ws.select(length(trim('id)).alias("len"), 'id).show(false)

      /*
       * concat array and take n elements,head,tail
       */
      val dd_ss = Seq((1, Seq("a", "b", "c")), (1, Seq("a", "b", "c"))).toDF("id", "arr")
      val dd_ss1 = Seq((1, Seq("a", "b", "c")), (1, Seq("a", "b", "c"))).toDF("id", "arr")

      //========get position of letter/character=========
      dd_ss.show(false)
      dd_ss.withColumn("position", instr('arr.cast("string"), "["))
        .selectExpr("*", "length(string(arr)) as length", "id * 10 as mul") //get the length of the array of string
        .show(10, false)

      //get only the tail from the list returns list
      def tal = udf {
        (f: Seq[String], l: Seq[String]) => (f ++ l).tail
      }

      //get only the head from the list return as string
      def hed = udf {
        (f: Seq[String], l: Seq[String]) => (f ++ l).tail
      }

      //get only the first n elements and returns list
      def tak(n: Int) = udf {
        (f: Seq[String], l: Seq[String]) => (f ++ l).take(n)
      }

      val nw_dd = dd_ss.alias("d").join(
        dd_ss1.alias("ds"), Seq("id")).
        select(
          'id,
          tal(dd_ss("arr"), dd_ss1("arr")).alias("tail"),
          hed(dd_ss("arr"), dd_ss1("arr")).as("head"),
          tak(4)(dd_ss("arr"), dd_ss1("arr")).as("take"))
      nw_dd.show(false)

      /*
       * filter not null
       */

      val df_nn = Seq((Some(1), None), (Some(2), Some(4)), (None, None))
        .toDF("id", "name")

      val df_nul = Seq((null.asInstanceOf[String], null.asInstanceOf[String]), ("ll", "ll"))
        .toDF("id", "name")

      //=========filter condition apply on all columns with not null operation=========
      val fil_cond = df_nul.columns.map(x => col(x).isNotNull).reduce(_ && _)
      val fil_or = df_nul.columns.map(x => col(x).isNull).reduce(_ || _)
      df_nul.filter(fil_cond).show(false)

      //========negate the filter condition use (!)=============
      df_nul.filter(!(fil_cond)).show(false)

      //===========not null===========
      df_nul.filter('id.isNotNull).show(false)

      /*
       * create named_struct(json) column
       */
      val dd_ns = Seq(("1", "ss"), ("2", "bb")).toDF("id", "nn")

      //======convert all columns to json============
      dd_ns.select('id).toJSON.toDF("jsn").show()

      //=====to convert only specific columns to json =========
      dd_ns.withColumn("dd", to_json(struct('id.alias("i")))).show(false)

      dd_ns.createOrReplaceTempView("ll")
      spark.sql("""select to_json(struct(id)) from ll""").show(false)

      //=====named_Struct,struct in spark scala
      dd_ns.selectExpr("named_struct('i',id) as jsn", "struct(id) as str")

      /*
       * check elements based on array
       */
      val df1 = Seq((1, 67), (2, 77), (3, 56), (4, 90)).toDF("soc", "ages")
      val z = Array(90, 56, 67)
      df1.withColumn(
        "flag", when('ages.isin(z: _*), "in Z array")
          .otherwise("not in Z array"))
        .show(false)

      /*
       * count rows on each partition
       */
      val dd_p = spark.sparkContext.parallelize(Seq(("1", "oi"), ("3", "i")), 10).toDF("id", "nam")

      //=====all partitions with number of rows======
      dd_p.rdd
        .mapPartitionsWithIndex { case (i, rows) => Iterator((i, rows.size)) }
        .toDF("partition_number", "rows")
        .show(false)

      //====only prints nonempty partitions with count=====
      dd_p.groupBy(spark_partition_id().alias("partition_id"))
        .count()
        .show(false)

      /*
       * get the execution time
       */
      spark.time(
        dd_p.groupBy('id).agg(collect_list("nam").as("cl")).select("*").show(false))

      /*
       * Sortby/orderby columns
       */
      dd_p.sort(desc("id"), desc("nam")).show(false)
      dd_p.orderBy('id asc, 'nam.desc).show(false)

      /*
       * get random values for each rows
       */
      val rnd_df = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("id", "ltr")
      rnd_df.withColumn("rand", lit(rand())).show(false)

      //get static rand value
      rnd_df.withColumn("ran", lit(scala.util.Random.nextDouble())).show(false)

      /*
       * Check if HDFS file exists
       */
      val conf_hd = spark.sparkContext.hadoopConfiguration
      val obj_hdfs = org.apache.hadoop.fs.FileSystem.get(conf_hd)
      val exists = obj_hdfs.exists(new org.apache.hadoop.fs.Path("/user/ymuppi1/orders.txt"))
      println(exists)

      /*
       * exceptall join
       */
      val ea = Seq((1, "a"), (2, "a"), (2, "a"), (2, "a"), (2, "b"), (3, "c")).toDF("x", "y")
      val ea1 = Seq((1, "a"), (1, "a")).toDF("x", "y")
      ea.exceptAll(ea1).show(false)

      /*
       * row_number,rank,dense_rank
       */

      ea.withColumn("row_number", row_number().over(Window.partitionBy('y).orderBy('x))).show(false)
      ea.withColumn("rank", rank().over(Window.orderBy('x))).show(false)
      ea.withColumn("dense_rank", dense_rank().over(Window.orderBy('x))).show(false)
      val df_dr = Seq(
        ("2019-01-29"),
        ("2019-01-29"),
        ("2019-07-31"),
        ("2019-01-29"),
        ("2019-07-31")).toDF("date")
      //======append partition value based on partition =========
      df_dr.withColumn("id", concat(lit("ABC"), dense_rank().over(Window.orderBy('date)))).show(false)

      //=====or=====
      val df_zwi = df_dr.distinct.rdd.zipWithIndex()
        .map { r => (r._1.getString(0), r._2 + 1) }
        .toDF("date", "p_id")
      df_dr.alias("den").join(df_zwi.alias("zip"), Seq("date"))
        .withColumn("id", concat(lit("ABC"), 'p_id))
        .show(false)

      /*
       * typed lit,map
       */
      ea.withColumn("da", typedLit(Map("foo" -> 1))).show(false)
      ea.withColumn("tl", typedLit("foo", 1, 2)).show(false)
      ea.withColumn("map", map(lit("id"), lit(1))).show(false)

      //=====lookup map value and add to column======
      val lookup_map = Map("1" -> "1234", "2" -> "3456")
      val testMapCol = typedLit(lookup_map)
      ea.withColumn("lookup", coalesce(testMapCol($"x"), lit(""))).show(false)

      /*
       * lag,lead window functin
       */
      val df_l = Seq(("xx", 1, "A"), ("yy", 2, "A"), ("zz", 1, "B"), ("yy", 3, "B"), ("tt", 4, "B")).toDF("id", "ts", "sess")
      df_l.groupBy($"sess").agg(collect_set('id).alias("cl")).select('sess, 'cl).show(false)
      println("lag" * 10)
      val lag_df = df_l.withColumn("lag", lag('id, 1).over(Window.partitionBy('sess).orderBy('ts)))
      lag_df.show(false)
      println("lead" * 10)
      val lead_df = df_l.withColumn("lead", lead('id, 1).over(Window.partitionBy('sess).orderBy('ts)))
      lead_df.show(false)

      //========group by and collect_list on lag column and nulls are removed by default==========
      lag_df.groupBy('id).agg(collect_list('lag).as("lag"))
        .select("*")
        .show(false)
      df_l.withColumn("cn_ws", concat_ws("|", array("*"))).show(false)
      val d_ws = Seq(("a", "b", Option.empty[String])).toDF("Col1", "Col2", "Col3")

      /*
       *concat_ws and select all columns and cast to string type
       */

      d_ws.na.fill("null")
        .withColumn("cn_ws", concat_ws("|", array("*")))
        .show(false)

      d_ws.na.fill("null")
        .withColumn("cn_ws1", concat_ws("|", array(d_ws.columns.map(c => col(c).cast(StringType)): _*)))
        .show(false)

      d_ws.select(d_ws.columns.map(col): _*)
        .show(false)

      /*
       * timestamp in milli seconds using sys time and spark function
       */
      d_ws.withColumn("sys_milli", lit(System.currentTimeMillis()))
        .withColumn("ts_milli", lit(current_timestamp().cast("timestamp")
          .cast("decimal(18,3)") * 1000)
          .cast("decimal(15,0)"))
        .show(false)

      /*
       * get summary(stats) of dataframe
       */

      d_ws.summary().show(false)

      /*
       * get configurations of current spark context
       */

      val ga = spark.conf.getAll
      ga.foreach(println)

      //====or====

      val gc_a = spark.sparkContext.getConf.getAll
      gc_a.foreach(println)

      //=====get specific conf from spark session======
      println(spark.conf.get("spark.sql.shuffle.partitions"))

      /*
       * getitem
       */
      val df_gi = Seq(("a_1", "xyz", "abc"), ("b_2", "xyz", "abc")).toDF("id1", "id2", "id3")
      df_gi.withColumn("_tmp", split('id1, "_"))
        .withColumn("id1_1", '_tmp.getItem(0))
        .withColumn("id1_2", '_tmp.getItem(1))
        .drop("_tmp")
        .show(false)

      /*
       * check all columns using case when
       */

      val lo = Seq((1, 2, 0), (0, 0, 1), (0, 0, 0), (1, 2, 0)).toDF("x", "y", "z")
      val cls = lo.columns
      lo.select(
        cls.map(c => when(col(c) === 0, 5).otherwise(col(c)).as(c)): _*)
        .show(false)

      /*
       * convert dataset to dataframe
       */

      //=======Dataset======
      val ds = Seq(("VAR1, VAR2, VAR3, VAR4"), ("a, b, c, d"), ("ae, f, g, h")).toDS()
      ds.show()

      //========read the dataset using .csv dataframe method========
      val ds_df = spark.read.option("inferSchema", "true").option("header", "true").csv(ds)
      ds_df.show()

      /*
       * zip
       */
      val df_z1 = Seq((1), (2), (3)).toDF("id")
      val df_z2 = Seq(("a"), ("b"), ("c")).toDF("id")
      val rd_z = df_z1.rdd.zip(df_z2.rdd)
      println(rd_z.collect.mkString("|"))
      rd_z.foreach(println)

      /*
       * list and filter files and rename file in hdfs
       */
      //      val all_files=FileSystem.get( spark.sparkContext.hadoopConfiguration ).listStatus(new Path("/user/ymuppi1/")).map(_.getPath()).mkString("|")
      //      val req_files=all_files.split("\\|").filter(_.contains(".csv")).mkString
      //      val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration);
      //      hdfs.rename(new Path("/user/ymuppi1/file_write/part-r-00000-64c8c8f2-444e-42a4-9095-5814a12ceded"), new Path("/user/ymuppi1/file_write/med.csv"))

      /*
       * split,count,getItem
       */
      val df_spl = Seq(("oi"), ("ll,io")).toDF()
      df_spl.withColumn("siz", size(split('value, ",")))
        .withColumn("first_ele", split('value, ",").getItem(0))
        .withColumn("sec_ele", split('value, ",").getItem(1))
        .show(false)
      lo.createOrReplaceTempView("lop")
      spark.sql("select * from lop").show(false)
      spark.sql("select distinct l.x,l.y,l.z from lop l join lop b on l.x=b.x limit 10").show(false)

      /*
       * stack
       */
      spark.sql("""with cte as (select stack(2,"2019-02-09 12:09:34.888","2019-02-04 12:09:34.888") as (ts)) 
        select max(ts) from cte""").show(false)

    }
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

