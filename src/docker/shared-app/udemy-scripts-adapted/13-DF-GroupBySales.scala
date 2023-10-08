import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 * 1. Simple
 ** 2. Grouping
 * 3. Windowing Aggregations
 */

val salesDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/sales-simple.csv")
salesDF.createOrReplaceTempView("sales")
/* Examples schema salesDF
root
 |-- orderId: integer (nullable = true)
 |-- customer: string (nullable = true)
 |-- country: string (nullable = true)
 |-- product: string (nullable = true)
 |-- dt: date (nullable = true)

Temporary view : sales
 */
salesDF.show
/* Number of customers per country */
// SQL Solution
spark.sql("""
            | select country, count(distinct customer) num_customers
            | from sales
            | group by country
            | order by num_customers desc
            |""".stripMargin).show()
// DF API
salesDF.groupBy("country" ).agg(
  countDistinct("customer").as("num_customers")
).sort($"num_customers".desc).
show()


/* Number of orders per country and product */
// SQL Solution
spark.sql("""
            | select country, product, count(orderId) num_orders
            | from sales
            | group by country, product
            | order by product, country, num_orders desc,
            |""".stripMargin).show()

spark.sql("""
            | select country, product, count(orderId) num_orders
            | from sales
            | group by country, product
            | order by  num_orders desc,  country, product
            |""".stripMargin).show()
// DF API
salesDF.groupBy("country" ).agg(
  countDistinct("customer").as("num_customers")
).sort($"num_customers".desc).
  show()


// DF API refactored for readability
val numCustomers = countDistinct("customer").as("num_customers")
salesDF.groupBy("country").agg(numCustomers).show(53,false)


/* Num orders per country, product having more than 1 order */
// SQL Solution
spark.sql("""
            | select country, product, count(*) as num_orders
            | from sales
            | group by country, product
            | having num_orders > 1
            | order by num_orders desc, product, country
            |""".stripMargin).show()


spark.sql("""
            | select customer, count(*) as num_orders
            | from sales
            | group by customer
            | order by customer
            |""".stripMargin).show()

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
