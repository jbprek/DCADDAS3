// Databricks notebook source
val customer1Df = spark.read.format("json").
option("inferSchema",true).
load("/data/customer.json")

customer1Df.printSchema
//.schema(customerDfSchema_ST)
// .schema(customerDfSchema_DDL)
// COMMAND ----------


// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val stuctType = StructType(
Array(
StructField("address_id",IntegerType,true),
                   StructField("birth_country",StringType,true), 
                   StructField("birthdate",DateType,true), 
                   StructField("customer_id",IntegerType,true),
                   StructField("demographics",
                               StructType(Array(
                                                StructField("buy_potential",StringType,true),
                                                StructField("credit_rating",StringType,true), 
                                                StructField("education_status",StringType,true), 
                                                StructField("income_range",ArrayType(IntegerType,true),true), 
                                                StructField("purchase_estimate",IntegerType,true), 
                                                StructField("vehicle_count",IntegerType,true))
                                         ),true),  
                   StructField("email_address",StringType,true),
                   StructField("firstname",StringType,true),
                   StructField("gender",StringType,true), 
//                   StructField("is_preffered_customer",StringType,true),
                   StructField("lastname",StringType,true),
                   StructField("salutation",StringType,true)
))

val customer2Df = spark.read.format("json").
  option("inferSchema",false).
  schema(stuctType).
  load("/data/customer.json")

customer2Df.printSchema

// COMMAND ----------
val ddlString =
  """
    |address_id INT,
    |birth_country STRING,
    |birthdate date,
    |customer_id INT,
    |demographics STRUCT<
    |buy_potential: STRING,
    |credit_rating: STRING,
    |education_status: STRING,
    |income_range: ARRAY<INT>,
    |purchase_estimate: INT,
    |vehicle_count: INT>,
    |email_address STRING,
    |firstname STRING,gender
    |STRING,is_preffered_customer
    |STRING,lastname STRING,
    |salutation STRING,
    |extraInfo STRING
    |""".stripMargin


val customer3Df = spark.read.format("json").
  option("inferSchema",false).
  schema(ddlString).
  load("/data/customer.json")

// COMMAND ----------

customer3Df.printSchema

// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

customerDf.printSchema()

// COMMAND ----------

display(customerDf)

// COMMAND ----------

// MAGIC %fs head /data/customer.json

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/learning
