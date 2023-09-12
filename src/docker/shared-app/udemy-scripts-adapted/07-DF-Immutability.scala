import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

customerDf.explain("formatted")

val Df = customerDf.
  withColumnRenamed("email_address", "mail").
  withColumn("developer_site", lit("insightahead.com")).
  drop("birth_country", "address_id").
  filter(dayofmonth($"birthdate") < 20)


Df.explain("formatted")
