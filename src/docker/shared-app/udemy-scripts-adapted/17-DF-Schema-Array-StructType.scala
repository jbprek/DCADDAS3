import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

// TODO add array of type
val schema = StructType(
  Seq(
    StructField("name", StringType, nullable = false),
    StructField("income_range", ArrayType(IntegerType), nullable = false),
    StructField("address", StructType(Seq(
      StructField("street", StringType),
      StructField("city", StringType)
    )),nullable=false)
  )
)
/*
root
 |-- name: string (nullable = false)
 |-- income_range: array (nullable = false)
 |    |-- element: integer (containsNull = true)
 |-- address: struct (nullable = false)
 |    |-- street: string (nullable = true)
 |    |-- city: string (nullable = true)
 */

val data = Seq(
  Row("John", Array(1000,1100), Row("Avenue Foch","Paris")),
  Row("Alice", Array(2000, 2200),Row("5th Avenue","New York")),
  Row("Bob", Array(3000, 3300),Row("Via Corso","Rome")))


val df =  spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

df.printSchema

// Use of explode
df.select($"name", explode($"income_range").as("income")).show

// Access array elements
df.select($"name", $"income_range"(0), col("income_range")(1)).show

// Access nested type attributes
df.select($"name", $"address.street", col("address.city")).show
