import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val df1 = List(
  ("john", "10"),
  ("peter", "11")
).toDF("name", "salary")


val df2 = List(
  ("john", "9"),
  ("peter", "11"),
  ("leonard", "12")
).toDF("name", "salary")

// NOTE union(df) and unionAll(df) give the same result, use distinct if unique results are needed
df1.union(df2).show
df1.unionAll(df2).show


df1.union(df2).distinct.show

val altColsDF = List(
  ( "9","john"),
  ("11","peter"),
  ( "12","leonard")
).toDF( "salary", "name")

// Union is semantically wrong since cols are not in order
df1.union(altColsDF).show
// Expected behavior
df1.unionByName(altColsDF).show