//spark-shell --num-executors 16 --executor-cores 2
import org.apache.spark.sql.functions.{spark_partition_id, asc, desc, array, concat, explode, floor, lit, rand}


//Making it a sort merge join by disabling storing the small table in memory. 
spark.sql("SET spark.sql.autoBroadcaseJoinedThreshold = -1")


val df_a = spark.read.csv("/data/dataset_a")
val df_b = spark.read.csv("/data/dataset_b")

df_b.createOrReplaceTempView("dataset_b")

val dataset_a_cols = df_a.columns.toSeq.mkString(",")
val df_arr_array = df_a.withColumn("range", sequence(lit(0), lit(16)))
df_arr_array.createOrReplaceTempView("dataset_a")

val df_a_salted = spark.sql("""
SELECT
  """ + dataset_a_cols + """
  ,explode(range) AS salted_key 
FROM dataset_a""")


val df_b_salted = spark.sql("""
SELECT
  b.*
  ,concat(b._c0, '_', FLOOR(RAND(123456)*16)) AS salted_key 
FROM dataset_b b""")

//val df_a_repart = df_a_salted.repartition(col("salted_key"))
val df_b_repart = df_b_salted.repartition(col("salted_key"))

//println(df_a_repart.rdd.getNumPartitions)
//println(df_b_repart.rdd.getNumPartitions)

df_a_salted.createOrReplaceTempView("dataset_a")
df_b_repart.createOrReplaceTempView("dataset_b")



spark.sql(s"""
SELECT
  a._c0 AS mykey
  ,count(a._c1) AS tot
FROM
  dataset_b b
  INNER JOIN dataset_a a
    ON b.salted_key = concat(a._c0, '_', a.salted_key)
GROUP BY a._c0
""").show(26)

/*
+-----+---+
|mykey|tot|
+-----+---+
|    l| 10|
|    x| 10|
|    g| 10|
|    m| 10|
|    f| 10|
|    n| 10|
|    k| 10|
|    v| 10|
|    e| 10|
|    o| 10|
|    h| 10|
|    z|100|
|    p| 10|
|    d| 10|
|    y| 10|
|    w| 10|
|    c| 10|
|    u| 10|
|    i| 10|
|    q| 10|
|    j| 10|
|    b| 10|
|    a| 10|
|    r| 10|
|    t| 10|
|    s| 10|
+-----+---+
*/
