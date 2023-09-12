import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 ** 1. Simple
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

