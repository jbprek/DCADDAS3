import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 * 1. Simple
 * 2. Grouping
 * 3. Windowing Aggregations
 */

/* SIMPLE Aggregations */
invoiceDF.select(
  count("*").as("Count *"),
  sum("Quantity").as("TotalQuantity"),
  avg("UnitPrice").as("AvgPrice"),
  countDistinct("InvoiceNo").as("CountDistinct")
).show()

//  SOS note that unlikely most of the other functions that expect Column argument,
//  aggregate functions have overloaded methods that support both String and Column
//  for a column argument.
invoiceDF.select(
  count($"*").as("Count *"),
  sum($"Quantity").as("TotalQuantity"),
  avg($"UnitPrice").as("AvgPrice"),
  countDistinct($"InvoiceNo").as("CountDistinct")
).show()
// SOS You can mix

// Usign expr
invoiceDF.selectExpr(
  "count(1) as `count 1`",
  "count(StockCode) as `count field`",
  "sum(Quantity) as TotalQuantity",
  "avg(UnitPrice) as AvgPrice"
).show()
// SQL
invoiceDF.createOrReplaceTempView("sales")
val summarySQL = spark.sql(
  """
    |SELECT Country, InvoiceNo,
    | sum(Quantity) as TotalQuantity,
    | round(sum(Quantity*UnitPrice),2) as InvoiceValue
    | FROM sales
    | GROUP BY Country, InvoiceNo
    |""".stripMargin)

summarySQL.show()


/* Grouping Aggregations */
val summaryDF = invoiceDF.groupBy("Country", "InvoiceNo")
  .agg(
    sum("Quantity").as("TotalQuantity"),
    round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue"),
    expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")
  )
summaryDF.show()

// Column vals to be used with agg, to verify readability
val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
val TotalQuantity = sum("Quantity").as("TotalQuantity")
val InvoiceValue = expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

val summaryDF = invoiceDF.
  withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm")).
  where(year($"InvoiceDate") === 2010).
  withColumn("WeekNumber", expr("weekofyear(InvoiceDate)")).
  groupBy("Country", "WeekNumber").
  agg(NumInvoices, TotalQuantity, InvoiceValue)

summaryDF.sort("Country", "WeekNumber").show()

/**
 * USE of summary and describe
 * def describe(cols: String*): DataFrame
 *
 *
Computes basic statistics for numeric and string columns, including count, mean, stddev, min, and max. If no columns are given, this function computes statistics for all numerical or string columns.

This function is meant for exploratory data analysis, as we make no guarantee about the backward compatibility of the schema of the resulting Dataset. If you want to programmatically compute summary statistics, use the agg function instead.

ds.describe("age", "height").show()

// output:
// summary age   height
// count   10.0  10.0
// mean    53.3  178.05
// stddev  11.6  15.7
// min     18.0  163.0
// max     92.0  192.0
 *
 * def summary(statistics: String*): DataFrame
 *
 * vailable statistics are:

count
mean
stddev
min
max
arbitrary approximate percentiles specified as a percentage (e.g. 75%)
count_distinct
approx_count_distinct
 */

summaryDF.agg(
  count("Country"),
  max("InvoiceValue"),
  min("InvoiceValue"),
  round(avg("InvoiceValue"),2),
  round(mean("InvoiceValue"),2)
).show

// COMMAND ----------

summaryDF.count

// COMMAND ----------

summaryDF.select(count("*")).show

// COMMAND ----------

summaryDF.select(count("ws_sales_price")).show

// COMMAND ----------

summaryDF.describe().show
summaryDF.describe("Country").show
summaryDF.describe("TotalQuantity","InvoiceValue").show

summaryDF.summary("stddev").show
// COMMAND ----------

display(webSalesDf.summary("stddev"))

// COMMAND ----------

display(customerDf.groupBy(
  customerDf("birth_country"), $"birthdate")
  .agg(count("*").as("count"))
  .sort(desc("count")))

// COMMAND ----------

display(addressDf)

// COMMAND ----------


/* Window Aggregations
*   General strategy
*  1. Identify your partioning columns
*  2. Identify your ordering requirement
*  3. Define your window start and end

* */

import org.apache.spark.sql.expressions.Window

val runningTotalWindow = Window.partitionBy("Country").
  orderBy("WeekNumber").
  rowsBetween(-2, Window.currentRow)

summaryDF.withColumn("RunningTotal",
  sum("InvoiceValue").over(runningTotalWindow)
).show()


//###################  OLD

orderDF.printSchema

// COMMAND ----------
orderDF.select(count("customer")).show
orderDF.agg(count("customer")).show
orderDF.groupBy("customer").agg(count("product").alias("item_count")).show
orderDF.groupBy("customer")
  .agg(
   count("product").alias("item_count"),
   min("unit_price").alias("cheaper"),
   max("unit_price").alias("most_expensive"),
  ).show

// COMMAND ----------

customerDf.select(year($"birthdate")).show

// COMMAND ----------

orderDF.agg(
 count("unit_price"),
 max("unit_price"),
 min("unit_price"),
 round(avg("unit_price"),2),
 round(mean("unit_price"),2)
).show

// COMMAND ----------

webSalesDf.count

// COMMAND ----------

webSalesDf.select(count("*")).show

// COMMAND ----------

webSalesDf.select(count("ws_sales_price")).show

// COMMAND ----------

display(webSalesDf.describe("ws_sales_price"))

// COMMAND ----------

display(webSalesDf.summary("stddev"))

// COMMAND ----------

display(customerDf.groupBy(
 customerDf("birth_country"), $"birthdate")
  .agg(count("*").as("count"))
  .sort(desc("count")))

// COMMAND ----------

display(addressDf)

// COMMAND ----------

