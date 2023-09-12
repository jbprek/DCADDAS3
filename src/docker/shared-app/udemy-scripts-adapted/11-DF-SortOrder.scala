import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/* Setup */
val df =  personDF.na.drop("any")

/**
 * def sort(sortExprs: Column*): Dataset[T] :Returns a new Dataset sorted by the given expressions.
 * def sort(sortCol: String, sortCols: String*): Dataset[T] : Returns a new Dataset sorted by the specified column, all in ascending order.
 *
 * orderBy same syntax as sort
 */
df.sort($"dob".desc, $"name".asc).show
df.sort(expr("dob desc"), expr("name")).show

// SOS With the use of String cannot specify sort order
df.sort("dob","name").show
