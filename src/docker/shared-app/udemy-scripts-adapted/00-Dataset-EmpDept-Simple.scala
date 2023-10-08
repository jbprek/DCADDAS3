import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val empDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/employees.csv")

empDF.createOrReplaceTempView("emp")

//
empDF.printSchema

empDF.show

val dptDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/departments.csv")

dptDF.createOrReplaceTempView("dept")

//
dptDF.printSchema

dptDF.show