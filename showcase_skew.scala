//spark-shell --num-executors 16 --executor-cores 2


import org.apache.spark.sql.functions.{spark_partition_id, asc, desc}

//Making it a sort merge join by disabling storing the small table in memory. 
spark.sql("SET spark.sql.autoBroadcaseJoinedThreshold = -1")

val df_a = spark.read.csv("/data/dataset_a")
val df_b = spark.read.csv("/data/dataset_b")

//val df_a_repart = df_a.repartition(4)
val df_b_repart = df_b.repartition(col("_c0"))

println(df_a.rdd.getNumPartitions)
println(df_b_repart.rdd.getNumPartitions)


df_a.createOrReplaceTempView("dataset_a")
df_b_repart.createOrReplaceTempView("dataset_b")


//Check the non-skewed data
/*
 * df_a_repart.groupBy(spark_partition_id)
.count()
.orderBy(asc("count"))
.show(200)

// find what partition has the skew
df_b_repart.groupBy(spark_partition_id)
.count()
.orderBy(asc("count"))
.show(200)
*/



spark.sql(s"""
SELECT
    a._c0 AS mykey
    ,count(a._c1) AS tot
FROM 
  dataset_a a
  INNER JOIN dataset_b b
   ON  a._c0 = b._c0
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
|    w| 10|
|    y| 10|
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







