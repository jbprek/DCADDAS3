/*
 * Write DataFrames into  CSV files
 * Read existing DF from io-examples-in, output to io-examples-out
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val gradeDF = spark.read.option("header", "true").csv("/data/io-examples-in/student-schema-simple/grades.csv")
val studentDF =  spark.read.option("header", "true").option("delimiter", "|").option("inferSchema", true).csv("/data/io-examples-in/student-schema-simple/students.csv")

/*
Write to CSV  with header, default options
 */
gradeDF.write.option("header", "true").csv("/data/io-examples-out/student-csv-simple/grades")

/*
Write to CSV  with header, delimiter '|', default options
 */
studentDF.write.option("header", "true").csv("/data/io-examples-out/student-csv-simple/students")
gradeDF.write.option("header", "true").csv("/data/io-examples-out/student-csv-simple/grades")
/*
 * Use of mode - Overwrite will overwrite a data file
 * defmode(saveMode: String): DataFrameWriter[T]
Specifies the behavior when data or table already exists. Options include:

- overwrite: overwrite the existing data.
- append: append the data.
- ignore: ignore the operation (i.e. no-op).
- error or errorifexists: default option, throw an exception at runtime.
*
* NOTE on append mode new CSV files will be created along side the old ones, timestamps will be adjusted to the latest time
*
 */
// Overwrite mode
val df = studentDF.filter($"StudentID" =!= 1)
df.write.option("header", "true").mode("overwrite").csv("/data/io-examples-out/student-csv-simple/students")
// Append mode
val df = studentDF.filter($"StudentID" =!= 1)
df.write.option("header", "true").mode("append").csv("/data/io-examples-out/student-csv-simple/students")
// CSV TODO Use schema - No Header

// TODO Parquet

// TODO JSON