import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/* Setup */

var data = List(
  ("John", "1990-02-01"),
  ("John", "1967-03-15"),
  ("Bill", "2002-06-09"),
  ("Sean", "1980-08-23"),
  ("Sophie", "1980-08-23")
)
val df = data.toDF("name","dob")


/**
 * def sort(sortExprs: Column*): Dataset[T] :Returns a new Dataset sorted by the given expressions.
 * def sort(sortCol: String, sortCols: String*): Dataset[T] : Returns a new Dataset sorted by the specified column, all in ascending order.
 *
 * orderBy same syntax as sort
 */
df.sort($"dob".desc, $"name".asc).show

// SOS With the use of String cannot specify sort order it's always ascending
df.sort("dob","name").show
