from pyspark.sql.functions import *
from pyspark.sql import *

#https://stackoverflow.com/questions/76052445/databricks-named-struct-strange-behaviour-with-backslash-in-value/76055810#76055810
df = spark.createDataFrame([('a',)],['a'])
df.withColumn("str",(expr('concat("DE844",decode("\\u005C", "utf-8"),"/374")'))).show(10,False)
#+---+----------+
#|a  |str       |
#+---+----------+
#|a  |DE844\/374|
#+---+----------+

#https://stackoverflow.com/questions/76055730/concat-all-distinct-value-of-several-columns-into-a-column-in-pyspark/76055988#76055988
df = spark.createDataFrame([('a','e','i'),('b','f','j'),('c','g','k'),('d','h','l')],['c1','c2','c3'])
w=Window.partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df2= df.withColumn("temp_c1",collect_set(col("c1")).over(w))\
  .withColumn("temp_c2",collect_set(col("c2")).over(w))\
  .withColumn("temp_c3",collect_set(col("c3")).over(w))\
  .withColumn("c4",array_join(sort_array(flatten(array(col("temp_c1"),col("temp_c2"),col("temp_c3")))),'')).\
    drop(*['temp_c1','temp_c2','temp_c3'])

df2.show(10,False)
# +---+---+---+------------+
# |c1 |c2 |c3 |c4          |
# +---+---+---+------------+
# |b  |f  |j  |abcdefghijkl|
# |c  |g  |k  |abcdefghijkl|
# |d  |h  |l  |abcdefghijkl|
# |a  |e  |i  |abcdefghijkl|
# +---+---+---+------------+

#https://stackoverflow.com/questions/76053061/creating-timestamp-column-using-pyspark/76055293#76055293
df = spark.createDataFrame([('2020-11-03','15:34:02')], ['Date', 'Times (Sting)'])
df.withColumn('desired column',to_timestamp(concat(col('Date'),lit(' '), col('Times (Sting)')))).show(10,False)
df.withColumn('desired column',to_timestamp(concat(col('Date'),lit(' '), col('Times (Sting)')))).printSchema()

#+----------+-------------+-------------------+
#|Date      |Times (Sting)|desired column     |
#+----------+-------------+-------------------+
#|2020-11-03|15:34:02     |2020-11-03 15:34:02|
#+----------+-------------+-------------------+
#
#root
# |-- Date: string (nullable = true)
# |-- Times (Sting): string (nullable = true)
# |-- desired column: timestamp (nullable = true)


#https://stackoverflow.com/questions/76051523/how-to-rename-the-columns-inside-nested-column-in-pyspark/76055120#76055120
#sample json
json = '{"product":{"{Color}":"a"}}'
df = spark.read.json( sc.parallelize([json]))

#create Color column by using `.withField` and copy the `{Color}`data
#use .dropFields to drop struct columns
df1= df.withColumn("product", df['product'].withField('Color',col('product.`{Color}`'))).\
withColumn("product", col("product").dropFields("`{Color}`"))
df1.printSchema()
df1.show(10,False)

#root
# |-- product: struct (nullable = true)
# |    |-- Color: string (nullable = true)
#
#+-------+
#|product|
#+-------+
#|{a}      |
#+-------+

#https://stackoverflow.com/questions/76050130/is-there-a-way-in-pyspark-to-copy-one-schema-to-another-dataframe
from pyspark.sql.functions import *
df1 = spark.createDataFrame([('2022-02-02',2,'a')],['A','B','C']).withColumn("A",to_date(col("A")))
print("df1 Schema")
df1.printSchema()
#df1 Schema
#root
# |-- A: date (nullable = true)
# |-- B: long (nullable = true)
# |-- C: string (nullable = true)

df2 = spark.createDataFrame([('2022-02-02','2','a')],['A','B','C'])
print("df2 Schema")
df2.printSchema()
#df2 Schema
#root
# |-- A: string (nullable = true)
# |-- B: string (nullable = true)
# |-- C: string (nullable = true)
#

#casting the df2 columns by getting df1 schema using select clause
df3 = df2.select(*[(col(x.name).cast(x.dataType)) for x in df1.schema.fields])
df3.show(10,False)
print("df3 Schema")
df3.printSchema()

#+----------+---+---+
#|A         |B  |C  |
#+----------+---+---+
#|2022-02-02|2  |a  |
#+----------+---+---+

#df3 Schema
#root
# |-- A: date (nullable = true)
# |-- B: long (nullable = true)
# |-- C: string (nullable = true)

#https://stackoverflow.com/questions/76057335/add-character-at-character-count-in-pyspark
df = spark.createDataFrame([("M202876QC0581AADMM01",)], ["str"])

pat = r"^(.{1})(.{6})(.{6})(.{2})(.+)"
df = df.withColumn("str", regexp_replace("str", pat, r"$1-$2-$3-$4-$5"))
#+------------------------+
#|str                     |
#+------------------------+
#|M-202876-QC0581-AA-DMM01|
#+------------------------+

#https://stackoverflow.com/questions/76059539/calculating-cumulative-sum-over-non-unique-list-elements-in-pyspark
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
data = [{"node": 'r1', "items": ['a','b','c','d'], "orderCol": 1},
        {"node": 'r2', "items": ['e','f','g','a'], "orderCol": 2},
        {"node": 'r3', "items": ['h','i','g','b'], "orderCol": 3},
        {"node": 'r4', "items": ['j','i','f','c'], "orderCol": 4},
        ]

w=Window.partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = spark.createDataFrame(data).\
  withColumn("temp_col",collect_list(col("items")).over(w)).\
  withColumn("cumulative_item_count",size(array_distinct(flatten(col("temp_col")))))
df.show(20,False)

#+------------+----+--------+--------------------------------------------------------+---------------------+
#|items       |node|orderCol|temp_col                                                |cumulative_item_count|
#+------------+----+--------+--------------------------------------------------------+---------------------+
#|[a, b, c, d]|r1  |1       |[[a, b, c, d]]                                          |4                    |
#|[e, f, g, a]|r2  |2       |[[a, b, c, d], [e, f, g, a]]                            |7                    |
#|[h, i, g, b]|r3  |3       |[[a, b, c, d], [e, f, g, a], [h, i, g, b]]              |9                    |
#|[j, i, f, c]|r4  |4       |[[a, b, c, d], [e, f, g, a], [h, i, g, b], [j, i, f, c]]|10                   |

#https://stackoverflow.com/questions/76061395/pass-column-names-dynamically-to-when-condition-to-check-is-null-condition-on-ea
null_chk_ls = map(lambda x: func.col(x).isNull(), primary_key_columns)
or_cond = reduce(lambda a, b: a|b, null_chk_ls)
# Column<'((id IS NULL) OR (label IS NULL))'>

data_sdf. \
    withColumn('action', 
               func.when(or_cond, func.lit('delete')).
               otherwise('no delete')
               ). \
    show()

# +----+-----+---------+
# |  id|label|   action|
# +----+-----+---------+
# |   1|  foo|no delete|
# |null|  bar|   delete|
# |   3| null|   delete|
# +----+-----+---------+

#https://stackoverflow.com/questions/76073778/pyspark-replace-null-values-with-the-mean-of-corresponding-row
'''count number of nulls and calculate the average value'''
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.createDataFrame([(1,2,3),(4,None,6),(7,8,None)],['col1','col2','col3'])
df.show(10,False)

df1 = df.withColumn("nulls_count",size(filter(array(*[isnull(col(c)) for c in df.columns]), lambda x: x))).\
  withColumn("arr_vals",array(*[coalesce(col(c),lit(0)) for c in df.columns])).\
  withColumn("sum_elems",expr("aggregate(arr_vals,cast(0 as bigint),(acc, x) -> acc + x)")).\
  withColumn("mean_val",expr('round(sum_elems/((size(arr_vals))-nulls_count),1)'))

df1.select([when(col(c).isNull(), col("mean_val")).otherwise(col(c)).alias(c) for c in df.columns]).\
  show(10, False)

#https://stackoverflow.com/questions/76073065/pyspark-read-complex-json-file
str = """{"tables":[{"name":"PrimaryResult","columns":[{"name":"TenantId","type":"string"},{"name":"TimeGenerated","type":"datetime"},{"name":"CorrelationId","type":"string"},{"name":"UserName","type":"string"}],"rows":[["1341234","5143534","14314123","test@test.cloud"]]}]}"""
df = spark.read.json(sc.parallelize([str]))
df = df.withColumn("tables", explode(col("tables"))).select("tables.*").withColumn("rows", explode(col("rows"))).withColumn("tmp", explode(arrays_zip("columns", "rows"))).select("tmp.columns.name", "tmp.rows")
df.groupBy(lit(1)).pivot("name").agg(first(col("rows"))).drop("1").show()

#https://stackoverflow.com/questions/76078849/explode-column-values-into-multiple-columns-in-pyspark
#explode array_rows to columns using pivot.
(
    df
    .withColumn('id', F.monotonically_increasing_id())
    .selectExpr('id', "explode(substitutes) AS S")
    .selectExpr('id', "'substitutes_' || S[0] AS col", "S[1] as val")
    .groupby('id').pivot('col').agg(F.first('val'))
    .drop('id')
)
# +-------------+-------------+-------------+-------------+-------------+
# |substitutes_1|substitutes_2|substitutes_3|substitutes_4|substitutes_5|
# +-------------+-------------+-------------+-------------+-------------+
# |    598319049|     38453298|   2007569845|         null|         null|
# |     30981733|         null|         null|         null|         null|
# |    102649381|     10294853|     10294854|     44749181|     35132896|
# |    849607426|    185158834|     11028011|     10309801|     11028010|
# |     10307642|     10307636|     15754215|     45612359|     10307635|
# |    730446111|    617024811|    665689309|    883699488|    159896736|
# |     10290923|     33282357|         null|         null|         null|
# |     43982130|     15556050|     15556051|     11961012|     16777263|
# |     10309216|         null|         null|         null|         null|
# |     21905160|     21609422|     21609417|     20554612|     20554601|
# +-------------+-------------+-------------+-------------+-------------+

#https://stackoverflow.com/questions/76076409/pyspark-reset-cumulative-sum-column-based-on-condition
#reset after flag is true for window
df = spark.createDataFrame(
    [
        (1001, "2023-04-01", False, 0, 0),
        (1001, "2023-04-02", False, 0, 0),
        (1001, "2023-04-03", False, 1, 1),
        (1001, "2023-04-04", False, 1, 1),
        (1001, "2023-04-05", True, 4, 3),
        (1001, "2023-04-06", False, 4, 3),
        (1001, "2023-04-07", False, 4, 3),
        (1001, "2023-04-08", False, 10, 6),
        (1001, "2023-04-09", True, 10, 0),
        (1001, "2023-04-10", False, 12, 2),
        (1001, "2023-04-11", False, 13, 3),
    ],
    ["id", "date", "reset", "cumsum", "new_cumsum"],
)

w = Window.orderBy("date")
w2 = Window.partitionBy("partition").orderBy("date")
df = df.withColumn("diff", col("cumsum") - lag("cumsum", default=0).over(w)) \
    .withColumn("partition", when(~col("reset"), 0).otherwise(1)) \
    .withColumn("partition", sum("partition").over(w)) \
    .withColumn("new_cumsum_2", sum(col("diff")).over(w2)).drop("diff", "partition")

df.show()
# +----+----------+-----+------+----------+------------+
# |  id|      date|reset|cumsum|new_cumsum|new_cumsum_2|
# +----+----------+-----+------+----------+------------+
# |1001|2023-04-01|false|     0|         0|           0|
# |1001|2023-04-02|false|     0|         0|           0|
# |1001|2023-04-03|false|     1|         1|           1|
# |1001|2023-04-04|false|     1|         1|           1|
# |1001|2023-04-05| true|     4|         3|           3|
# |1001|2023-04-06|false|     4|         3|           3|
# |1001|2023-04-07|false|     4|         3|           3|
# |1001|2023-04-08|false|    10|         6|           9|
# |1001|2023-04-09| true|    10|         0|           0|
# |1001|2023-04-10|false|    12|         2|           2|
# |1001|2023-04-11|false|    13|         3|           3|
# +----+----------+-----+------+----------+------------+


#https://stackoverflow.com/questions/76078929/flattening-nested-json-file-into-a-pyspark-df
#key-value pair using stack function
from pyspark.sql.functions import *
str= """{"verifiedManifest":{"AB1":{"quantity":1}, "DE5":{"quantity":5}, "AG1":{"quantity":10}}}"""
df = spark.read.json(sc.parallelize([str]))
df1 = df.select("verifiedManifest.*")
sz_cols = len(df1.columns).__format__('')
stack_expr = f"stack({sz_cols}" +"," +','.join([f'"{f}",{f}.*' for f in df1.columns]) + ") as (verifiedManifest_name, verifiedManifest_quantity)"
df1.select(expr(stack_expr)).show(10,False)
# +---------------------+-------------------------+
# |verifiedManifest_name|verifiedManifest_quantity|
# +---------------------+-------------------------+
# |AB1                  |1                        |
# |AG1                  |10                       |
# |DE5                  |5                        |
# +---------------------+-------------------------+


#https://stackoverflow.com/questions/76081217/transform-multiple-rows-into-single-row-multiple-columns
#pivot by adding column names alias

df = spark.createDataFrame([('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','814735847','0'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','596343371','1'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','814735847',None),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','222288904','3'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','183692578','2'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','222288904',None),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','708542578','303519145','4'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','152507105','390801409','0'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','152507105','129141834','3'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','152507105','900045087','1'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','152507105','280043267','2'),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','152507105','390801409',None),('7d6d7b44-440b-45d7-b25a-3d55817a889b','#','152507105','335625185','4')],['id','orderid','item','cpid','pos'])

df.show()
df.withColumn("pos", expr('concat("pos_",pos,"_cpid")')).\
  groupBy("id","orderid","item").pivot("pos").agg(first(col("cpid")).alias("pos_cpid")).drop('null').\
    show(10,False)

#+------------------------------------+-------+---------+----------+----------+----------+----------+----------+
#|id                                  |orderid|item     |pos_0_cpid|pos_1_cpid|pos_2_cpid|pos_3_cpid|pos_4_cpid|
#+------------------------------------+-------+---------+----------+----------+----------+----------+----------+
#|7d6d7b44-440b-45d7-b25a-3d55817a889b|#      |152507105|390801409 |900045087 |280043267 |129141834 |335625185 |
#|7d6d7b44-440b-45d7-b25a-3d55817a889b|#      |708542578|814735847 |596343371 |183692578 |222288904 |303519145 |
#+------------------------------------+-------+---------+----------+----------+----------+----------+----------+ 
#
# 
# https://stackoverflow.com/questions/76084300/pyspark-pivot-dataframe/76084988#76084988
df = spark.createDataFrame([
("TenantId", "TennatId_1"),
("TimeGenerated", "2023-04-17T11:50:51.9013145Z"),
("ActivityType", "Connection"),
("CorrelationId", "608dd49a"),
("UserName", "test_1@test.cloud"),
("Name", "Name1"),
("Source", "Client"),
("Parameters", "{}"),
("SourceSystem", "Azure"),
("Type", "Check"),
("_ResourceId", "/subscriptions/5286ce"),
("TenantId", "TennatId_2"),
("TimeGenerated", "2023-04-17T11:50:51.944022Z"),
("ActivityType", "Connection"),
("CorrelationId", "11c0d75f0000"),
("UserName", "test_2@test.cloud"),
("Name", "Name2"),
("Source", "Client"),
("Parameters", "{}"),
("SourceSystem", "Azure"),
("Type", "Check"),
("_ResourceId", "/subscriptions/5286ce38-272f-4c54")], ["name", "rows"]) 
from pyspark.sql.functions import *
df.groupBy(lit(1)).pivot("name").agg(first(col("rows"))).drop("1").show(10,False)
#+------------+-------------+-----+----------+------+------------+----------+----------------------------+-----+-----------------+---------------------+
#|ActivityType|CorrelationId|Name |Parameters|Source|SourceSystem|TenantId  |TimeGenerated               |Type |UserName         |_ResourceId          |
#+------------+-------------+-----+----------+------+------------+----------+----------------------------+-----+-----------------+---------------------+
#|Connection  |608dd49a     |Name1|{}        |Client|Azure       |TennatId_1|2023-04-17T11:50:51.9013145Z|Check|test_1@test.cloud|/subscriptions/5286ce|
#+------------+-------------+-----+----------+------+------------+----------+----------------------------+-----+-----------------+---------------------+   
#define window
w=Window.partitionBy(lit("1")).orderBy("mid")

#add order id column and temporary window partition
df1 = df.withColumn("mid",monotonically_increasing_id()).\
  withColumn("temp_win", when(col("rows").rlike("^TennatId"),lit(1)).otherwise(lit(0))).\
  withColumn("windw", sum(col("temp_win")).over(w))

#pivot and window
df1.groupBy("windw").pivot("name").agg(first(col("rows"))).drop("windw").show(10,False)

#+------------+-------------+-----+----------+------+------------+----------+----------------------------+-----+-----------------+---------------------------------+
#|ActivityType|CorrelationId|Name |Parameters|Source|SourceSystem|TenantId  |TimeGenerated               |Type |UserName         |_ResourceId                      |
#+------------+-------------+-----+----------+------+------------+----------+----------------------------+-----+-----------------+---------------------------------+
#|Connection  |608dd49a     |Name1|{}        |Client|Azure       |TennatId_1|2023-04-17T11:50:51.9013145Z|Check|test_1@test.cloud|/subscriptions/5286ce            |
#|Connection  |11c0d75f0000 |Name2|{}        |Client|Azure       |TennatId_2|2023-04-17T11:50:51.944022Z |Check|test_2@test.cloud|/subscriptions/5286ce38-272f-4c54|
#+------------+-------------+-----+----------+------+------------+----------+----------------------------+-----+-----------------+---------------------------------+

#https://stackoverflow.com/questions/76092684/how-to-get-the-line-number-of-each-line-across-multiple-files-in-spark/76092926#76092926
from pyspark.sql import *
from pyspark.sql.functions import *
df1 = df1.withColumn("mid", monotonically_increasing_id())
windowSpec = W.orderBy("mid")
df1 = df1.withColumn("line_num", row_number().over(windowSpec)).show()

#---
#https://stackoverflow.com/questions/76104265/pyspark-dataframe-collect-list-for-overlaping-windows-starting-at-each-non-null/76106639#76106639
#Create an index column by using monotonically_increasing_id and then Divide the row_number() with 4.
#Finally collect_list if v1 is not null for the window current_row, unboundedFollowing.
from pyspark.sql.functions import *
from pyspark.sql import *
df = spark.createDataFrame([('a','2000-01-01',None,'0.1'),
('a','2000-01-02',1,'0.2'),
('a','2000-01-03',None,'0.3'),
('a','2000-01-04',None,'0.4'),
('a','2000-01-05',2,'0.5'),
('a','2000-01-06',None,'0.6'),
('a','2000-01-07',None,'0.7'),
('b','2000-01-08',None,'0.8')],['name','date','v1','v2'])

windowSpec = Window.orderBy("mid")
windowSpec2 = Window.partitionBy("wndw").orderBy("mid").rowsBetween(Window.currentRow, Window.unboundedFollowing)

df = df.withColumn("mid",monotonically_increasing_id()).\
  withColumn("wndw", ceil(row_number().over(windowSpec)/4)).\
  withColumn("acc", when(col("v1").isNotNull(),collect_list(col("v2")).over(windowSpec2)).otherwise(array(lit(None))))

df.show(10,False)

#+----+----------+----+---+------------+----+--------------------+
#|name|date      |v1  |v2 |mid         |wndw|acc                 |
#+----+----------+----+---+------------+----+--------------------+
#|a   |2000-01-01|null|0.1|25769803776 |1   |[null]              |
#|a   |2000-01-02|1   |0.2|60129542144 |1   |[0.2, 0.3, 0.4]     |
#|a   |2000-01-03|null|0.3|94489280512 |1   |[null]              |
#|a   |2000-01-04|null|0.4|128849018880|1   |[null]              |
#|a   |2000-01-05|2   |0.5|163208757248|2   |[0.5, 0.6, 0.7, 0.8]|
#|a   |2000-01-06|null|0.6|197568495616|2   |[null]              |
#|a   |2000-01-07|null|0.7|231928233984|2   |[null]              |
#|b   |2000-01-08|null|0.8|266287972352|2   |[null]              |
#+----+----------+----+---+------------+----+--------------------+