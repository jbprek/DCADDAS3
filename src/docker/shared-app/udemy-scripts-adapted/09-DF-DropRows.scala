import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * # 1 USE of distinct and dropDuplicates
 * def distinct(): Dataset[T]
 *
 *  SOS Remember no Column Type arguments
Returns a new Dataset that contains only the unique rows from this Dataset. This is an alias for dropDuplicates.
 Equality checking is performed directly on the encoded representation of the data and thus is not affected by a custom equals function defined on T.

 def dropDuplicates(): Dataset[T]
Returns a new Dataset that contains only the unique rows from this Dataset. This is an alias for distinct.

 def dropDuplicates(col1: String, cols: String*): Dataset[T]

def dropDuplicates(colNames: Array[String]): Dataset[T]

def dropDuplicates(colNames: Seq[String]): Dataset[T]


 */
val demoDf = List(
 (1,"Monday"),
 (1,"Monday"),
 (2,"Tuesday"),
 (3,"Tuesday"),
 (3,"Wednesday"),
 (4,"Thursday"),
 (5,"Friday"),
 (6,"Saturday"),
 (7,"Sunday")).toDF("id","name")


demoDf.show
demoDf.distinct.show
demoDf.dropDuplicates.show

demoDf.dropDuplicates("name").show
demoDf.dropDuplicates("name", "id").show
demoDf.dropDuplicates(List("name", "id")).show
demoDf.dropDuplicates(Array("name", "id")).show

