import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val salesInDF = spark.read.format("csv").
  option("header", "true").option("inferSchema", "true").
  load("/data/sales-simple.csv")

salesDF.printSchema

// Add autoincrement orderId column

val salesDF = salesInDF.withColumn("orderId",monotonically_increasing_id())