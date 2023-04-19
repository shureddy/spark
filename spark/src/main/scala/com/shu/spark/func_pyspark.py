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