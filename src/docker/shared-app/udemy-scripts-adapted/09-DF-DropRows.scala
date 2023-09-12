import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * # 1 USE of distinct and dropDuplicates
 * def distinct(): Dataset[T]
Returns a new Dataset that contains only the unique rows from this Dataset. This is an alias for dropDuplicates.
 Equality checking is performed directly on the encoded representation of the data and thus is not affected by a custom equals function defined on T.

 def dropDuplicates(): Dataset[T]
Returns a new Dataset that contains only the unique rows from this Dataset. This is an alias for distinct.

 def dropDuplicates(col1: String, cols: String*): Dataset[T]

def dropDuplicates(colNames: Array[String]): Dataset[T]

def dropDuplicates(colNames: Seq[String]): Dataset[T]

 SOS Remember no Column Type arguments
 */

demoDf.show
demoDf.distinct.show
demoDf.dropDuplicates.show

demoDf.dropDuplicates("name").show
demoDf.dropDuplicates("name", "id").show
demoDf.dropDuplicates(List("name", "id")).show
demoDf.dropDuplicates(Array("name", "id")).show


/**
 *  # 2 Use of na functions
 *  - na.drop(how = "all") drops rows where all values are null
 *  - na.drop(how = "any") drops rows where any value is null
 *  - an extra Seq() can specify the range of columns for the any or all criteria
 *  - SOS remember how is mandatory an can take values "all" or "any"
 */
personDF.show
// drops rows where all column values are null
personDF.na.drop(how = "all").show
// drops rows where any column value is null
personDF.na.drop(how = "any").show.
// drops rows where in specific column values, all values are null, I.e drop all rows where id and name are null
personDF.na.drop(how = "all",Seq("id","name")).show
// drops rows where in specific column values, any value is null, I.e drop all rows where id or name is null
personDF.na.drop(how = "any",Seq("id","name")).show
