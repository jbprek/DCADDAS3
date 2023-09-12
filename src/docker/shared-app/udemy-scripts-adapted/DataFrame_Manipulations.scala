import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._




display(Dfn.na.fill(1234))

// COMMAND ----------

display(Dfn.na.fill(Map("salutation" -> "UNKNOW", "firstname" -> "John", "lastname" -> "Doe", "birthyear" -> 9999)))


// COMMAND ----------


// COMMAND ----------

// COMMAND ----------

display(customerDf
  .na.drop("any")
  .sort($"firstname", $"lastname".desc)
  .select("firstname", "lastname", "birthdate"))

// COMMAND ----------
display(customerDf)

// COMMAND ----------

val customerWithAddress =
  customerDf.join(addressDf, customerDf.col("address_id") === addressDf.col("address_id"), "inner")
    .select("customer_id"
      , "firstname"
      , "lastname"
      , "demographics.education_status"
      , "location_type"
      , "country"
      , "city"
      , "street_name"
    )

// COMMAND ----------

display(customerWithAddress)

// COMMAND ----------

webSalesDf.select(countDistinct("ws_item_sk")).show

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/dcad_data

// COMMAND ----------

webSalesDf

// COMMAND ----------

webSalesDf.where("ws_bill_customer_sk = 45721").count

// COMMAND ----------

// display(
webSalesDf.join(customerDf, $"customer_id" === $"ws_bill_customer_sk", "left")
  .select("customer_id", "ws_bill_customer_sk", "*")
  .where("customer_id is null")
  .count
//   )

// COMMAND ----------

webSalesDf.where("ws_bill_customer_sk is null").count

// COMMAND ----------

customerDf.select("firstname", "lastname", "customer_id")

// COMMAND ----------

val df1 = customerDf
  .select("firstname", "lastname", "customer_id")
// .withColumn("from",lit("df1"))

// COMMAND ----------

val df2 = customerDf
  .select("lastname", "firstname", "customer_id")
// .withColumn("from",lit("df2"))

// COMMAND ----------

df1.unionByName(df2).distinct.where("customer_id = 45721").show

// COMMAND ----------

val customerWithAddr = customerDf
  .join(addressDf, customerDf("address_id") === addressDf("address_id"))

// COMMAND ----------

customerWithAddr.count

// COMMAND ----------

customerWithAddr.cache.count

// COMMAND ----------

customerWithAddr.count

// COMMAND ----------

customerWithAddr.unpersist.count

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

// COMMAND ----------

customerWithAddr.persist(StorageLevel.MEMORY_AND_DISK).count

// COMMAND ----------

customerWithAddr.count

// COMMAND ----------

customerWithAddr.storageLevel

// COMMAND ----------

customerWithAddr.unpersist.count

// COMMAND ----------

customerWithAddr.persist(StorageLevel.MEMORY_ONLY_SER_2).count

// COMMAND ----------

customerWithAddr.storageLevel

// COMMAND ----------

customerWithAddr.storageLevel

// COMMAND ----------

customerWithAddr.show
