import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * # 1 USE na.fill
 *
def fill(valueMap: Map[String, Any]): DataFrame
 Returns a new DataFrame that replaces null values.

SCALA-TYPE(s)
- Boolean
- String
- Double
- Long


def fill(value: <SCALA-TYPE> , cols: Array[String]): DataFrame
Returns a new DataFrame that replaces null or NaN values in specified <SCALA-TYPE> columns.

def fill(value: <SCALA-TYPE>, cols: Seq[String]): DataFrame
(Scala-specific) Returns a new DataFrame that replaces null or NaN values in specified <SCALA-TYPE> columns.

def fill(value: <SCALA-TYPE>): DataFrame
Returns a new DataFrame that replaces null or NaN values in <SCALA-TYPE> columns with value.

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
 *
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
