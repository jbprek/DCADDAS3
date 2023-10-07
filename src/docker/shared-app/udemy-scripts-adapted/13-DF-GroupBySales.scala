import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 * 1. Simple
 ** 2. Grouping
 * 3. Windowing Aggregations
 */


/* Examples schema salesDF
root
 |-- orderId: integer (nullable = true)
 |-- customerId: string (nullable = true)
 |-- country: string (nullable = true)
 |-- productId: string (nullable = true)
 |-- date: date (nullable = true)

Temporary view sales

Definitions in 00-Dataset-Sales-Simple.scala
 */


/* Number of customers per country */


/* Number of customers per country */
// SQL Solution
spark.sql("""
            | select country, count(distinct customerId) num_customers
            | from sales
            | group by country
            | order by num_customers desc
            |""".stripMargin).show(53,false)
// DF API
salesDF.groupBy("country" ).agg(
  countDistinct("customerId").as("num_customers")
).show(53,false)

// DF API refactored for readability
val numCustomers = countDistinct("customerId").as("num_customers")
salesDF.groupBy("country").agg(numCustomers).show(53,false)


/* Num orders per country, product having more than 1 order */
// SQL Solution
spark.sql("""
            | select country, productId, count(*) as num_orders
            | from sales
            | group by country, productId
            | having num_orders > 1
            | order by num_orders desc, productId, country
            |""".stripMargin).show(50)


spark.sql("""
            | select customerId, count(*) as num_orders
            | from sales
            | group by customerId
            | order by customerId
            |""".stripMargin).show(51)

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
