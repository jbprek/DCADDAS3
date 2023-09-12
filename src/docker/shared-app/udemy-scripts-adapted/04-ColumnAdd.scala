import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/* Add a column with derived/calculated value */
val df1 = customerDf.withColumn("fullname", expr("concat_ws(' ', firstname, lastname)"))
// Alternative
val df1 = customerDf.withColumn("fullname", concat_ws(" ", $"firstname",$"lastname") )

df1.printSchema
df1.select('fullname).show(false)

/* Add a column with a constant.literal values */
val df2 = customerDf.withColumn("legacy_customer", lit("Y"))
df2.printSchema
df2.select('legacy_customer).show(false)