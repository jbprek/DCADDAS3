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

/* SIMPLE Aggregations */
salesDF.select(
  count("*").as("Count *"),
  sum("amount").as("TotalAmount"),
  round(avg("amount"),2).as("AvgAmount"),
  countDistinct("Country").as("NumCountries"),
  countDistinct("product").as("NumProducts"),
  countDistinct("customer").as("NumCustomers"),
).show()

//  SOS note that unlikely most of the other functions that expect Column argument,
//  aggregate functions have overloaded methods that support both String and Column
//  for a column argument.


// Usign expr
salesDF.selectExpr(
  "count(1) as `count 1`",
  "count(*) as `count *`", // Same as above
  "sum(amount) as TotalAmount",
  "round(avg(amount),2) as AvgAmount",
  "count(distinct country) as NumCountries",
  "count(distinct product) as NumProducts",
  "count(distinct customer) as NumCustomers"
).show()
// SQL
spark.sql(
  """
    |SELECT
    | count(1) as `count 1`,
    | count(*) as `count *`,
    | sum(amount) as TotalAmount,
    | round(avg(amount),2) as AvgAmount,
    | count(distinct country) as NumCountries,
    | count(distinct product) as NumProducts,
    | count(distinct customer) as NumCustomers
    | FROM sales
    |""".stripMargin).show



