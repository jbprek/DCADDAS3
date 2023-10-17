/*
 * Read file from CSV
 * Read student schema from CSV under /data/student-schema-simple
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val gradeDF = spark.read.option("header", "true").csv("/data/io-examples-in/student-schema-simple/grades.csv")
// OR
val gradeDF = spark.read.format("csv").
  option("header", "true").option("delimiter",",").option("inferSchema", false).
  load("/data/io-examples-in/student-schema-simple/grades.csv")

gradeDF.printSchema
/*
root
|-- GradeID: string (nullable = true)
|-- StudentID: string (nullable = true)
|-- Course: string (nullable = true)
|-- Grade: string (nullable = true)
*/

// Note the change in schema with option("inferSchema", true)
val gradeDF = spark.read.option("header", "true").option("inferSchema", true).csv("/data/io-examples-in/student-schema-simple/grades.csv")
gradeDF.printSchema
/*
scala> gradeDF.printSchema
root
 |-- GradeID: integer (nullable = true)
 |-- StudentID: integer (nullable = true)
 |-- Course: string (nullable = true)
 |-- Grade: double (nullable = true)
 */
// Delimiter '|'
val studentDF =  spark.read.option("header", "true").option("delimiter", "|").option("inferSchema", true).csv("/data/io-examples-in/student-schema-simple/students.csv")
/*
scala> studentDF.printSchema
root
|-- StudentID: integer (nullable = true)
|-- FirstName: string (nullable = true)
|-- LastName: string (nullable = true)
|-- BirthDate: date (nullable = true)
*/

// CSV TODO Use schema - No Header

// TODO Parquet

// TODO JSON