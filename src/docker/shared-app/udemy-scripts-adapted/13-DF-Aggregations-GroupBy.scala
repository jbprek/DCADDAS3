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
 |-- dt: string (nullable = true)
 |-- amount: double (nullable = true)

Temporary view : sales
 */
salesDF.show
/*
+-------+--------+-------+-------+--------------+------+
|orderId|customer|country|product|            dt|amount|
+-------+--------+-------+-------+--------------+------+
|      1|    Jean| France|      A|    2023-01-02| 100.0|
|      2|  Gianni|  Italy|      A|    2023-02-10| 100.0|
|      3|   Luigi|  Italy|      C|    2023-03-07| 300.0|
|      4|  Pierre| France|      B|    2023-03-31| 200.0|
|      5|   Peter|    USA|      A|    2023-04-01| 110.0|
|      6|     Ron|    USA|      A|    2023-04-01| 110.0|
|      7|    John|    USA|      C|2023-04-20.330|  NULL|
|      8|    John|    USA|      A|    2023-05-01| 110.0|
|      9|   Peter|    USA|      B|    2023-05-05| 220.0|
|     10|   Luigi|  Italy|      C|    2023-05-23| 330.0|
|     11|     Ron|    USA|      B|    2023-06-05| 180.0|
|     12|  Pierre| France|      B|    2023-06-15| 220.0|
|     13|   Peter|    USA|      C|    2023-07-20| 280.0|
|     14|    Jean| France|      A|    2023-08-07| 120.0|
|     15|     Ron|    USA|      C|    2023-08-20| 300.0|
|     16|    John|    USA|      A|    2023-09-05| 100.0|
|     17|    Jean| France|      B|    2023-09-07| 240.0|
+-------+--------+-------+-------+--------------+------+
 */
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


/* Number of orders , average amount, max amount, min amount first order and last order dates  per country and product  */
// SQL Solution
spark.sql("""
            | select country,
            | product,
            | count(orderId) num_orders,
            | round(avg(amount),0) avg_amount,
            | min(amount) min_amount,
            | max(amount) max_amount,
            | min(dt) first_order_date,
            | max(dt) first_order_date
            | from sales
            | group by country, product
            | order by country, product, num_orders desc
            |""".stripMargin).show()

// DF API
salesDF.groupBy("country", "product" ).agg(
  count("orderId").as("num_orders"),
  avg("amount").as("avg_amount"),
  min("amount").as("min_amount"),
  max("amount").as("max_amount"),
  min("dt").as("first_order_date"),
  max("dt").as("last_order_date")
).sort($"country",$"product", $"num_orders".desc).show()


// DF API refactored for readability
val countOrderAgg = count("orderId").as("num_orders")
val minDtAgg = min("dt").as("first_order_date")
salesDF.groupBy("country", "product").agg(countOrderAgg, minDtAgg).
sort($"country",$"product",$"num_orders".desc).show()

// DF Simple alternative no column rename
salesDF.groupBy("country", "product" ).agg(
  "orderId" -> "count",
  "dt" -> "min"
).show()