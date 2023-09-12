import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
