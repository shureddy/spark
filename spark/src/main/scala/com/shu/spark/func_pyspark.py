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

#https://stackoverflow.com/questions/76123779/pyspark-trimming-all-columns-in-a-dataframe-to-100-characters/76124187#76124187
#substring on all columns
from pyspark.sql.functions import *
df = spark.createDataFrame([('a','1','a')],['i','j','k'])
df.select([substring(col(f),0,100).alias(f) for f in df.columns]).show(10,False)
#+---+---+---+
#|i  |j  |k  |
#+---+---+---+
#|a  |1  |a  |
#+---+---+---+

#https://stackoverflow.com/questions/76122909/join-two-tables-while-multiplying-columns-x-rows-in-scala
#create aggregate records
cols = ['item1', 'item2', 'item3']
df = spark.createDataFrame([('user1',[1,2,3]),('user2',[4,5,6]),('user3',[7,8,9])],['user_id','value'])
df1 = spark.createDataFrame([([0.5,0.6,0.7],[0.2,0.3,0.4],[0.1,0.8,0.9])],['item1','item2','item3'])
df2 = df.join(df1).select('user_id', *[arrays_zip(col('value'), c).alias(c) for c in cols])

df2.select('user_id',"item1","item2","item3",*[aggregate(c, lit(0.0), lambda acc, x: acc + x['value'] * x[c]).alias(c+"i") for c in cols]).show(10,False)
#+-------+------------------------------+------------------------------+------------------------------+------------------+------+------+
#|user_id|item1                         |item2                         |item3                         |item1i            |item2i|item3i|
#+-------+------------------------------+------------------------------+------------------------------+------------------+------+------+
#|user1  |[{1, 0.5}, {2, 0.6}, {3, 0.7}]|[{1, 0.2}, {2, 0.3}, {3, 0.4}]|[{1, 0.1}, {2, 0.8}, {3, 0.9}]|3.8               |2.0   |4.4   |
#|user2  |[{4, 0.5}, {5, 0.6}, {6, 0.7}]|[{4, 0.2}, {5, 0.3}, {6, 0.4}]|[{4, 0.1}, {5, 0.8}, {6, 0.9}]|9.2               |4.7   |9.8   |
#|user3  |[{7, 0.5}, {8, 0.6}, {9, 0.7}]|[{7, 0.2}, {8, 0.3}, {9, 0.4}]|[{7, 0.1}, {8, 0.8}, {9, 0.9}]|14.600000000000001|7.4   |15.2  |
#+-------+------------------------------+------------------------------+------------------------------+------------------+------+------+


#https://stackoverflow.com/questions/76134363/parsing-json-with-pyspark-transformations

import json

simple_json = {}

with open("test_2.json") as file:
    data = json.load(file)

lst = []
for k, v in data.items():
    lst.append(v)
simple_json["results"] = lst

rddjson = sc.parallelize([simple_json])
df = sqlContext.read.json(rddjson, multiLine=True)
df.show()

from pyspark.sql import functions as F
df.select(F.explode(df.results).alias('results')).select('results.*').show(truncate=False)


#https://stackoverflow.com/questions/76168647/flatten-pyspark-dataframe
#json struct values dynamic flatten.
df = spark.createDataFrame([('someval','{"column1": "value1"}')],['key', 'content'])

sch=StructType([StructField("column1",StringType(),True)])

df.withColumn("tmp", from_json(col("content"), sch)).\
  select("key","tmp.*").\
    show(10,False)

df1 = df.select('key', from_json('content', MapType(StringType(), StringType())))

df2 = df1.select('key', explode('entries').alias('cols','rows'))

df2.groupBy('key').pivot('cols').agg(first('rows')).show()
# +-------+-------+
# |key    |column1|
# +-------+-------+
# |someval|value1 |
# +-------+-------+


import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, MapType
from pyspark.sql.functions import *

jsonString1 = """
  [
    {
      "person_id": 1,
      "address": {}
    }
  ]
"""

jsonString2 = """
  [
    {
      "person_id": 1,
      "address": {
        "line1": {
            "foo": "bar"
          }
      }
    }
  ]
"""
#https://stackoverflow.com/questions/76165920/how-can-i-skip-commands-from-running-if-row-doesnt-meet-some-criteria
# json when array is empty.
# Define a schema so no error is given when `address` is empty
schema = StructType([
    StructField("person_id", IntegerType(), True),
    StructField(
      "address", 
      MapType(
        StringType(), MapType(StringType(), StringType())), True)
])
pdf = pd.read_json(jsonString1)
df = spark.createDataFrame(pdf, schema=schema)

df1 = (df
       .select(col("person_id"), col("address"))
       .withColumn(
           "tmp",
           when(size(col("address")) > 0,  # use when for the case address is empty
                col("address")).otherwise(
               lit(None).cast(   
MapType(StringType(), MapType(StringType(), StringType())))
)
       )
       .select(col("person_id"), explode(col("tmp")).alias("key", "exploded_address"))
       .select(col("person_id"), col("exploded_address"))
       # other commands
       )

df1.collect()

#https://stackoverflow.com/questions/76180934/flatten-nested-json-struct-with-dynamic-keys-in-pyspark
#dynamic key value flatten in json doc
#id	json_column
#1	"{1: {amount: 1, time: 2}, 2: {amount: 10, time: 5}}"
#2	"{3: {amount: 1, time: 2}, 4: {amount: 10, time: 5}"

map_schema=MapType(StringType(), StructType([\
    StructField('amount', StringType(), True),\
    StructField('time', StringType(),True)\
]));

df\
.withColumn("json_column", F.from_json(F.col("json_column"), map_schema, {"allowUnquotedFieldNames":"true"}))\
.select("*", F.explode("json_column").alias("key", "value"))\
.select("id", "value.*")\
.show(truncate=False)

#dynamic stack and flatten the data.
#https://stackoverflow.com/questions/76202343/pyspark-aggregation-based-on-key-and-value-expanded-in-multiple-columns
df = spark.createDataFrame([(100,1,'A',5,'X',10,'L',20),(100,2,'B',5,'L',10,'A',20)],['id1','item','code_1','value_1','code_2','value_2','code_3','value_3'])

req_cols = [c for c in df.columns if c.startswith("code_") or c.startswith("value_")]

sql_expr = "stack("+ str(int(len(req_cols)/2))+"," +','.join(req_cols) +")"

df.show()
df1 = df.select("id1",'item',expr(f"{sql_expr}")).\
  groupBy("id1","col0").agg(sum("col1").alias("sum")).\
    withColumn("col0",concat(lit("sum_"),col("col0")))
df.select("id1","item").distinct().\
  join(df1,['id1']).\
    groupBy("id1","item").\
      pivot("col0").\
        agg(first(col("sum").alias("sum_"))).\
        show()

#window on id and sum on two columns
df = df.select('id1', 'item', 
               *[F.sum(x).over(Window.partitionBy('id1')).alias(f'sum_{x}') 
                 for x in df.columns if x not in ['id1', 'item']])
# +---+----+------+-------+------+-------+------+-------+
# |id1|item|code_1|value_1|code_2|value_2|code_3|value_3|
# +---+----+------+-------+------+-------+------+-------+
# |100|   1|     A|      5|     X|     10|     L|     20|
# |100|   2|     B|      5|     L|     10|     A|     20|
# +---+----+------+-------+------+-------+------+-------+

# +---+----+-----+-----+-----+-----+
# |id1|item|sum_A|sum_B|sum_L|sum_X|
# +---+----+-----+-----+-----+-----+
# |100|   2|   25|    5|   30|   10|
# |100|   1|   25|    5|   30|   10|
# +---+----+-----+-----+-----+-----+

#https://stackoverflow.com/questions/76208282/append-a-new-column-list-of-values-in-pyspark/76209479#76209479
#append list to the dataframe

from pyspark.sql.functions import *
from pyspark.sql.types import *
df= spark.createDataFrame([('Emma Larter',34),('Mia Junior',59),('Sophia',32),('James',40)],['Name','Age'])
df_ind = spark.createDataFrame(df.rdd.zipWithIndex(),['val','ind'])
Salary = [35000, 24000, 55000, 40000]
df_salary = spark.createDataFrame(spark.createDataFrame(Salary, IntegerType()).rdd.zipWithIndex(),['val1','ind'])
df_ind.join(df_salary,['ind']).select("val.*","val1.*").drop('ind').show()

#+-----------+---+-----+
#|       Name|Age|value|
#+-----------+---+-----+
#|Emma Larter| 34|35000|
#| Mia Junior| 59|24000|
#|     Sophia| 32|55000|
#|      James| 40|40000|
#+-----------+---+-----+

#https://stackoverflow.com/questions/76234403/spark-merging-the-schema-of-two-array-columns-with-different-struct-fields
#union two nested types with different schema
df.union(
  df2
    .withColumn("LineItems",
      expr("transform(LineItems, x -> named_struct('ItemID', x.ItemID, 'Name', x.Name, 'DiscountRate', null))")
    )
)

#https://stackoverflow.com/questions/76247075/how-can-i-label-rows-before-and-after-an-event-in-pyspark
#window find event and mark all the item before as before
#after the event happend as after and event as event using window
df = spark.createDataFrame([(1,1657610298,0),
(1,1657610299,0),
(1,1657610300,0),
(1,1657610301,1),
(1,1657610302,0),
(1,1657610303,0),
(1,1657610304,0),
(2,1657610298,0),
(2,1657610299,0),
(2,1657610300,0),
(2,1657610301,1),
(2,1657610302,0),
(2,1657610303,0),
(2,1657610304,0)],['ID','timestamp','event'])
from pyspark.sql import *
from pyspark.sql.functions import *
df_windw = df.withColumn("temp_col",max(col("Event")).over(Window.partitionBy('ID').orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow))).\
  withColumn("type2", when((col("event")== 0) & (col("temp_col")==0),lit("before")).\
    when((col("event")== 0) & (col("temp_col")==1),lit("after")).\
      otherwise(lit("event"))).\
        drop("temp_col")
df_windw.show(100,False)

#order by inner rank
#https://stackoverflow.com/questions/76256099/pyspark-window-function-to-generate-rank-on-data-based-on-sequence-of-the-value
from pyspark.sql import *
from pyspark.sql.functions import *
simpleData = [
    ("X", 11, "typeA"),
    ("X", 12, "typeA"),
    ("X", 13, "typeB"),
    ("X", 14, "typeB"),
    ("X", 15, "typeC"),
    ("X", 16, "typeC"),
    ("Y", 17, "typeA"),
    ("Y", 18, "typeA"),
    ("Y", 19, "typeB"),
    ("Y", 20, "typeB"),
    ("Y", 21, "typeC"),
    ("Y", 22, "typeC"),
]

schema = ["A", "B", "type"]
df = spark.createDataFrame(data=simpleData, schema=schema)

w1 = Window.partitionBy("A", "type").orderBy("B")

dfWithInnerRank = df.withColumn("innerRank", sum(lit(1)).over(w1))

w2 = Window.partitionBy("A").orderBy("innerRank")

dfWithInnerRank.withColumn("rank", row_number().over(w2)).show()
# +---+---+-----+---------+----+
# |  A|  B| type|innerRank|rank|
# +---+---+-----+---------+----+
# |  X| 11|typeA|        1|   1|
# |  X| 13|typeB|        1|   2|
# |  X| 15|typeC|        1|   3|
# |  X| 12|typeA|        2|   4|
# |  X| 14|typeB|        2|   5|
# |  X| 16|typeC|        2|   6|
# |  Y| 17|typeA|        1|   1|
# |  Y| 19|typeB|        1|   2|
# |  Y| 21|typeC|        1|   3|
# |  Y| 18|typeA|        2|   4|
# |  Y| 20|typeB|        2|   5|
# |  Y| 22|typeC|        2|   6|
# +---+---+-----+---------+----+


#endswith spark function
address = [(1,"14851 Jeffrey Rd","DE"),(2,"43421 Margarita St","NY"),(3,"13111 Siemon Ave","CA"),(4,"110 South Ave","FL")]
df= spark.createDataFrame(address,["id","address","state"])
df.show()
df.withColumn("address",
when(col("address").endswith("Rd"),regexp_replace(col("address"),"Rd","Road"))\
.when(col("address").endswith("St"),regexp_replace(col("address"),"St","Street"))\
.when(col("address").endswith("Ave"),regexp_replace(col("address"),"Ave","Avenue"))\
.otherwise(col("address")))\
.show(10,False)


#endswith spark function
from pyspark.sql.functions import *
df = spark.createDataFrame([('hp1_model1_min',1),('hp1_model1_pressure',1),('hp1_model3_max',1)],['itemName','itemValue'])
df.filter((col("itemname").endswith('min')) | (col("itemname").endswith('pressure'))).show(10,False)

#https://adb-2809981990316081.1.azuredatabricks.net/?o=2809981990316081#notebook/2118714825770110/command/1831513359037945
#concat_ws all columns
from pyspark.sql.functions import *
df = spark.createDataFrame([(1, "foo"),(2, "bar"),],["id", "label"])

df.withColumn("temp", concat_ws(" ", *df.columns)).groupBy(lit(1)).agg(array_join(collect_list(col("temp"))," ").alias("new_column")).\
  drop("1").\
  show(10,False)

#https://stackoverflow.com/questions/76280511/pyspark-group-rows-with-sequential-numbers-with-duplicates#76280870
#create new column for adjecent numbers and group them as collect_list
df = spark.createDataFrame(
    [(0, 'A'),
     (1, 'B'),
     (1, 'C'),
     (5, 'D'),
     (8, 'A'),
     (9, 'F'),
     (20, 'T'),
     (20, 'S'),
     (21, 'C')],
    ['time_slot', 'customer'])
window = Window.orderBy("time_slot")
df = df.withColumn("prev_time_slot", lag(col('time_slot')).over(window))
df = df.withColumn("isNewSequence", 
                   (col("time_slot") - col("prev_time_slot") > 1).cast("int"))
df = df.withColumn("groupId",sum("isNewSequence").over(window))
df_grouped = df.groupBy("groupId").agg(collect_list("time_slot").alias("grouped_slots"), 
                                        collect_list("customer").alias("grouped_customers"))
df_grouped.show(truncate=False)

# +-------+-------------+-----------------+
# |groupId|grouped_slots|grouped_customers|
# +-------+-------------+-----------------+
# |null   |[0]          |[A]              |
# |0      |[1, 1]       |[C, B]           |
# |1      |[5]          |[D]              |
# |2      |[8, 9]       |[A, F]           |
# |3      |[20, 20, 21] |[T, S, C]        |
# +-------+-------------+-----------------+


#create json output with out column name
#https://stackoverflow.com/questions/76280090/how-do-i-read-a-json-as-a-string-or-dictionary-from-a-column-in-spark-dataframe
#save as text file to remove the header false.
from pyspark.sql.functions import *
df = spark.createDataFrame([(1, "foo"),(2, "bar"),],["id", "label"])

df1= df.withColumn("temp", concat_ws(" ", *df.columns)).groupBy(lit(1)).agg(array_join(collect_list(col("temp"))," ").alias("new_column")).drop("1")

print(df1.select(struct(col("new_column")).alias("new")).toJSON().collect()[0])
#{"new":{"new_column":"1 foo 2 bar"}}
df.coalesce(1).write.format("text").option("header", "false").save("output.txt")

#https://stackoverflow.com/questions/76313809/spark-handle-json-with-dynamically-named-subschema/76314739#76314739
#Spark handle json with dynamically named subschema
#changing column names to make it unique
from pyspark.sql.functions import *
json_string = """{
    "10712": {
        "id": "10712",
        "age": 27,
        "gender": "male"
    },
    "217": {
        "id": "217",
        "age": 60,
        "gender": "female"
    }
}"""

df.printSchema()
df = spark.read.json(sc.parallelize([json_string]), multiLine=True)
cols = [ f"`{i}`" for i in df.columns]
col_len = len(cols)
stack_expr = ','.join(cols)
df.select(expr(f"stack({col_len},{stack_expr})")).\
  groupBy(lit(1)).\
    agg(to_json(collect_list(col("col0"))).alias("user")).drop("1").\
      show(10,False)

df.select(expr(f"stack({col_len},{stack_expr})")).\
  groupBy(lit(1)).\
    agg(collect_list(col("col0")).alias("user")).drop("1").\
      printSchema()
# root
#  |-- 10712: struct (nullable = true)
#  |    |-- age: long (nullable = true)
#  |    |-- gender: string (nullable = true)
#  |    |-- id: string (nullable = true)
#  |-- 217: struct (nullable = true)
#  |    |-- age: long (nullable = true)
#  |    |-- gender: string (nullable = true)
#  |    |-- id: string (nullable = true)

# +---------------------------------------------------------------------------------+
# |user                                                                             |
# +---------------------------------------------------------------------------------+
# |[{"age":27,"gender":"male","id":"10712"},{"age":60,"gender":"female","id":"217"}]|
# +---------------------------------------------------------------------------------+

# root
#  |-- user: array (nullable = false)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- age: long (nullable = true)
#  |    |    |-- gender: string (nullable = true)

#pivot on columns and use inline
#https://stackoverflow.com/questions/76335205/pyspark-transpose-grouped-data-frame
from pyspark.sql.functions import *
df = spark.createDataFrame([('one','word1'),
('one','word2'),
('one','word3'),
('two','word4'),
('two','word5'),
('two','word6'),
('three','word7'),
('three','word8'),
('three','word9')],['group','words'])


df = (df.groupby(lit('words').alias('group'))
      .pivot('group')
      .agg(collect_list('words')))
pivot_cols = [x for x in df.columns if x != 'group']
df = df.select('group', expr(f"inline(arrays_zip({','.join(pivot_cols)}))"))
df.show(100,False)
#+-----+-----+-----+-----+
#|group|one  |three|two  |
#+-----+-----+-----+-----+
#|words|word2|word9|word5|
#|words|word3|word7|word4|
#|words|word1|word8|word6|
#+-----+-----+-----+-----+
