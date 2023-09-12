import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// Original Schema

customerDf.printSchema


/* Use of withColumnRenamed(String,String) */
val renDf = customerDf.withColumnRenamed("email_address", "mail")
renDf.printSchema

// SOS Note cannot pass Column object  - gives error
val failDf = customerDf.withColumnRenamed($"email_address", "mail")

// SOS Attempt to rename a non-existing column will return without an error
val noOpDf = customerDf.withColumnRenamed("UKNOWN_COLUMN", "mail")
renDf.printSchema // same as customerDf.printSchema

/* Use of withColumnsRenamed(Map[String,String]) */
val multRenDf = customerDf.withColumnsRenamed(
  Map("firstname"->"first", "lastname"->"last"))
multRenDf.printSchema
