import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 * 1. Simple
 ** 2. Grouping
 * 3. Windowing Aggregations
 */

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
