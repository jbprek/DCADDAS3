/*
 * Simple Dataframe creation from a List of tuples using toDF to define name of columns
 *  NOTE all String are nullable, primitive types are not nullable
 */
val list = List((1,"john", true),(2, "Peter",false))

val df = list.toDF("id", "name","married")
// Alternative
val df = spark.createDataset(list).toDF("id", "name","married")
// Note the following works as well
val df = spark.createDataFrame(list).toDF("id", "name","married")
df.show(false)
df.printSchema
/*
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- married: boolean (nullable = false)
 */

/*
 * Create a Dataframe from Row List using a schema
 */
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, DataFrame}

val rowList = List(Row(1,"john", true),Row(2, "Peter",false))

val schema = StructType(
  Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("flag", BooleanType, nullable = false)
  )
)

// Create a DataFrame from the list and schema
val schDF:DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rowList), schema)

schDF.show(false)
schDF.printSchema
