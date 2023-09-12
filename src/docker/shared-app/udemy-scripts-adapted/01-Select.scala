import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/* # Select by column name string */
// Not possible to access array elemaent
val dfS1 = customerDf.select("firstname", "lastname", "demographics", "demographics.credit_rating", "demographics.income_range")
dfS1.printSchema
dfS1.show(false)

// WildCard using *
customerDf.select("*").printSchema

/* # Select by column objects */
val dfS2 = customerDf.select(
  $"firstname",
  'lastname,
  col("demographics"),
  customerDf("demographics.credit_rating"),
  column("demographics.income_range"),
  $"demographics.income_range"(0),
  expr("salutation"),
  expr("concat(firstname,lastname) name"))
dfS2.printSchema
dfS2.show(false)
// Do not mix in select arguments string and Column objects
customerDf.select("firstname", $"lastname") // Should fail

// Wild card works
customerDf.select($"*")

/* # Using selectExpr*/
val dfS3 = customerDf.selectExpr("birthdate birthday", "year(birthdate) birthyear")
dfS3.printSchema
dfS3.show(false)
