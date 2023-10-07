import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



val data = List(
  ("2021-12-01", "2021-01-01 12:33:00")
)
val inDf = spark.createDataFrame(data).toDF("dt", "dtm")

/* Convert Strings to Date, Timestamp */
//
val df = inDf.select($"dt".cast("Date"), $"dtm".cast("TIMESTAMP"))
// OR
val df = inDf.select($"dt".cast(DateType), $"dtm".cast(TimestampType))
// OR
val df = inDf.selectExpr("to_date(dt) as dt", "to_timestamp(dtm) as dtm")

df.printSchema
df.show

/* Current Date curdate() OR  current_date()*/
df.withColumn("now", curdate()).select("now").show
// OR
df.withColumn("now", current_date()).select("now").show
/* Current Timestamp now() OR current_timestamp() */
df.withColumn("now", now()).select("now").show(false)
// OR
df.withColumn("now", current_timestamp()).select("now").show(false)


/* Convert format Dates to String */




/* Convert format Timestamps  to String */

/* Parse extended formats Date */

/* Parse extended formats Timestamps */

/* Date extract year,month,date, date of year, year week from Date */

/* Compare Dates */

/* Compare Timestamps */

/* To Unix EPOCH to_unix_timestamp */
df.withColumn("now", to_unix_timestamp(now())).select("now").show(false)

/* From Unix EPOCH */

val testDF =
val salesStatDf = webSalesDf.select(
  "ws_order_number",
  "ws_item_sk",
  "ws_quantity",
  "ws_net_paid",
  "ws_net_profit",
  "ws_wholesale_cost"
)

// Using Column objects
val salesPerf1Df = salesStatDf.
  withColumn("expected_net_paid", $"ws_quantity" * $"ws_wholesale_cost").
  withColumn("calculated_profit", $"ws_net_paid" - $"expected_net_paid").
  withColumn("unit_price", $"ws_wholesale_cost" / $"ws_quantity").
  withColumn("rounded_unitPrice", round($"unit_price", 2)).
  withColumn("brounded_unitPrice", bround($"unit_price", 3))

salesPerf1Df.printSchema
salesPerf1Df.show(false)

// Using SQL in expr
val salesPerf2Df = salesStatDf.
  withColumn("expected_net_paid", expr("ws_quantity * ws_wholesale_cost")).
  withColumn("calculated_profit", expr("ws_net_paid - expected_net_paid")).
  withColumn("unit_price", expr("ws_wholesale_cost / ws_quantity")).
  withColumn("rounded_unitPrice", expr("round(unit_price, 2)")).
  withColumn("brounded_unitPrice", expr("bround(unit_price, 3)"))

salesPerf2Df.printSchema
salesPerf2Df.show(false)


// SOS Arithmetic operation can be used only in Arithmetic values, not ie in string columsn
// The below will fail silently resulting in a fullname column with null values.
val df1 = customerDf.withColumn("fullname", $"firstname"+$"lastname").select("fullname").show
