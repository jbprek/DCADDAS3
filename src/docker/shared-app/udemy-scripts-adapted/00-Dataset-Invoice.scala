import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val invoiceDF = spark.read.format("csv").
  option("header", "true").option("inferSchema", "true").
  load("/data/invoices.csv")

invoiceDF.printSchema