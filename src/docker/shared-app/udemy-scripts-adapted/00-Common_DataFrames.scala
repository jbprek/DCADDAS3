import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

// Databricks notebook source
val datFileReadOptions = Map("header" -> "true"
             ,"delimiter" -> "|"
             ,"inferSchema" -> "false")

val csvFileReadOptions = Map("header" -> "true"
             ,"delimiter" -> ","
             ,"inferSchema" -> "false")

// COMMAND ----------

val webSalesSchema = "ws_sold_date_sk LONG,ws_sold_time_sk LONG,ws_ship_date_sk LONG,ws_item_sk LONG,ws_bill_customer_sk LONG,ws_bill_cdemo_sk LONG,ws_bill_hdemo_sk LONG,ws_bill_addr_sk LONG,ws_ship_customer_sk LONG,ws_ship_cdemo_sk LONG,ws_ship_hdemo_sk LONG,ws_ship_addr_sk LONG,ws_web_page_sk LONG,ws_web_site_sk LONG,ws_ship_mode_sk LONG,ws_warehouse_sk LONG,ws_promo_sk LONG,ws_order_number LONG,ws_quantity INT,ws_wholesale_cost decimal(7,2),ws_list_price decimal(7,2),ws_sales_price decimal(7,2),ws_ext_discount_amt decimal(7,2),ws_ext_sales_price decimal(7,2),ws_ext_wholesale_cost decimal(7,2),ws_ext_list_price decimal(7,2),ws_ext_tax decimal(7,2),ws_coupon_amt decimal(7,2),ws_ext_ship_cost decimal(7,2),ws_net_paid decimal(7,2),ws_net_paid_inc_tax decimal(7,2),ws_net_paid_inc_ship decimal(7,2),ws_net_paid_inc_ship_tax decimal(7,2),ws_net_profit decimal(7,2)"

// COMMAND ----------

val customerDfSchema_DDL = "address_id INT,birth_country STRING,birthdate date,customer_id INT,demographics STRUCT<buy_potential: STRING, credit_rating: STRING, education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT, vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,is_preffered_customer STRING,lastname STRING,salutation STRING"

// COMMAND ----------

val itemSchema = "i_item_sk LONG,i_item_id STRING,i_rec_start_date date,i_rec_end_date date,i_item_desc STRING,i_current_price decimal(7,2),i_wholesale_cost decimal(7,2),i_brand_id INT,i_brand STRING,i_class_id INT,i_class STRING,i_category_id INT,i_category STRING,i_manufact_id INT,i_manufact STRING,i_size STRING,i_formulation STRING,i_color STRING,i_units STRING,i_container STRING,i_manager_id INT,i_product_name STRING"

// COMMAND ----------

val customerDf = spark.read.format("json").schema(customerDfSchema_DDL).load("/data/customer.json")

// COMMAND ----------

val webSalesDf = spark.read.options(csvFileReadOptions).schema(webSalesSchema).csv("/data/sales_web.csv")

// COMMAND ----------

val addressDf = spark.read.parquet("/data/address.parquet")

// COMMAND ----------

val itemDf = spark.read.options(datFileReadOptions).schema(itemSchema).csv("/data/item.dat")


val demoDf = List(
  (1,"Monday"),
  (1,"Monday"),
  (2,"Tuesday"),
  (3,"Tuesday"),
  (3,"Wednesday"),
  (4,"Thursday"),
  (5,"Friday"),
  (6,"Saturday"),
  (7,"Sunday")).toDF("id","name")


val personSchema =  StructType(Seq(
  StructField("id",IntegerType,nullable=false),
  StructField("name",StringType),
  StructField("dob",DateType)
))

val personDF = spark.read.
  schema(personSchema).
  option("header","false").
  option("inferSchema", "false").
  csv("/data/persons.csv")


val invoiceDF = spark.read.format("csv").
  option("header", "true").option("inferSchema", "true").
  load("/data/invoices.csv")


val gradesDF = spark.read.format("csv").
  option("header", "true").option("inferSchema", "true").
  load("/data/grades.csv")

val employeesDF = spark.read.
  option("header", "true").option("inferSchema", "true").
    csv("/data/employees.csv")


val employeeSchema = StructType(Seq(
  StructField("employee_id",IntegerType,true),
  StructField("first_name",StringType,true),
  StructField("last_name",StringType,true),
  StructField("department_id",IntegerType,true),
  StructField("salary", IntegerType, true),
  StructField("languages",ArrayType(StringType),true)
))

val employeesDF = spark.read.
  option("header", "true").schema(employeeSchema).
  csv("/data/employees.csv")


import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object WriteListToDataFrame {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("WriteListToDataFrame")
      .master("local[*]") // Replace with your cluster configuration
      .getOrCreate()

    // Define the schema for the DataFrame
    val schema = new StructType()
      .add(StructField("id", IntegerType, nullable = false))
      .add(StructField("name", StringType, nullable = true))

    // Create a native Scala List
    val data = List((1, "John"), (2, "Jane"), (3, "Mike"))

    // Convert the List to a DataFrame
    val df: DataFrame = spark.createDataFrame(data).toDF("id", "name")

    // Show the DataFrame
    df.show()

    // Stop the Spark session
    spark.stop()
  }
}


customerDf.printSchema
webSalesDf.printSchema
addressDf.printSchema
itemDf.printSchema
personDF.printSchema
demoDf.printSchema
invoiceDF.printSchema
