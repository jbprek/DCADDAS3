import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// SOS Applicable only on numberic columns

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
