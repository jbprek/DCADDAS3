import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val personSchema = StructType(Seq(
 StructField("id", IntegerType, nullable = false),
 StructField("name", StringType),
 StructField("dob", StringType)
))

val personDF = spark.read.
  schema(personSchema).
  option("header","false").
  option("inferSchema", "false").
  csv("/data/persons.csv")

/*
scala> personDF.show
+----+-------+----------+
|  id|   name|       dob|
+----+-------+----------+
|   1|   John|1990-01-01|
|   2|  Peter|2000-01-02|
|   3|  Laura|1991-01-03|
|   4|  Maria|2001-01-04|
|   5|     X1|      NULL|
|   6|   NULL|2001-01-04|
|   7|   NULL|      NULL|
|NULL|   NULL|      NULL|
|   8|  Johan|1992-01-01|
|   9|   Paul|2000-01-01|
|  10|Maureen|1990-01-04|
+----+-------+----------+
*/

/*
 * # 1 USE na.fill
 *
def fill(valueMap: Map[String, Any]): DataFrame
 Returns a new DataFrame that replaces null values.

def fill(value: <SCALA-VALUE>, cols: Seq[String]): DataFrame
def fill(value: <SCALA-VALUE> , cols: Array[String]): DataFrame
Returns a new DataFrame that replaces null or NaN values in specified <SCALA-VALUE> columns.
SCALA-VALUE(s)
- Boolean
- String
- Double
- Long
*/
personDF.na.fill(Map("id"-> -1 ,"name"->"Unkown","dob"->"1970-01-01")).show
personDF.na.fill(-1 , List("id")).show
personDF.na.fill(-1 , Array("id")).show
personDF.na.fill("Unknown" , Seq("name","dob")).show


/*
 * # 2 USE na.replace
 *
def replace(String,: Map[String, Any]): DataFrame  Returns a new DataFrame that substitutes in a column  values according to the map.
def replace(Seq(String),: Map[String, Any]): DataFrame  Returns a new DataFrame that substitutes in a column  values according to the map.
def replace(Array(String),: Map[String, Any]): DataFrame  Returns a new DataFrame that substitutes in a column  values according to the map.
*/
personDF.na.replace("id", Map(1-> 11 , 2-> 22)).show
personDF.na.replace(Array("name","dob"), Map("X1"->"Unknown" , "2000-01-01"-> "New Century")).show
personDF.na.replace(Seq("name","dob"), Map("X1"->"Unknown" , "2000-01-01"-> "Mew Century")).show

/**
 *  # 3 Use of na drop
 *  - na.drop(how = "all") drops rows where all column values are null
 *  - na.drop(how = "any") drops rows where any column value is null
 *
 */


// drops rows where all column values are null
personDF.na.drop(how = "all").show
// drops rows where any column value is null
personDF.na.drop(how = "any").show
// drops rows where in specific column values, all values are null, I.e drop all rows where id and name are null
personDF.na.drop(how = "all",Seq("id","name")).show
// drops rows where in specific column values, any value is null, I.e drop all rows where id or name is null
personDF.na.drop(how = "any",Seq("id","name")).show
