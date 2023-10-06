import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val invoiceDF = spark.read.format("csv").
  option("header", "true").option("inferSchema", "true").
  load("/data/invoices.csv")

invoiceDF.printSchema

// Mini set 0.001 %
val invDF = invoiceDF.filter($"Country" =!= "United Kingdom").
sample(0.0011, 3651482897L)