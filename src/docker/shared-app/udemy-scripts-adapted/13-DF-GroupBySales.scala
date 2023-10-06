import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 * 1. Simple
 ** 2. Grouping
 * 3. Windowing Aggregations
 */

/* Number of customers per country */
salesDF.createOrReplaceTempView("sales")

// SQL Solution
spark.sql("""
            | select country, count(customerId) num_customers
            | from sales
            | group by country
            |""".stripMargin).show
// DF API
salesDF.groupBy("country" ).agg(
  count("customerId").as("num_customers")
).show()

// DF API refactored for readability
val numCustomers = countDistinct("customerId").as("num_customers")
salesDF.groupBy("country" ).agg(numCustomers).show()
/* Grouping Aggregations */
val summaryDF = invoiceDF.groupBy("Country", "InvoiceNo").agg(
    sum("Quantity").as("TotalQuantity"),
    round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue"),
    expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")
  )
summaryDF.show()

// Column vals to be used with agg, to verify readability
val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
val TotalQuantity = sum("Quantity").as("TotalQuantity")
val InvoiceValue = expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

val exSummaryDF = invoiceDF.
  withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm")).
  where(year($"InvoiceDate") === 2010).
  withColumn("WeekNumber", expr("weekofyear(InvoiceDate)")).
  groupBy("Country", "WeekNumber").
  agg(NumInvoices, TotalQuantity, InvoiceValue)

exSummaryDF.sort("Country", "WeekNumber").show()
