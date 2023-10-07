import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/* 3 Ways
 - Use of cast(to: String): Column
 - Use of cast(to: DataType): Column
 - SQL function cast(V as TYPE) in selectExpr
 */

customerDf.select($"address_id", $"birthdate").printSchema

/* Use of cast(to: String): Column
* The supported types are:
* - string,
* - boolean,
* - byte,
* - short,
* - int,
* - long,
* -  float,
* - double,
* - decimal,
* - date,
* - timestamp
*
* SOS NOTE that to is CASE INSENSITIVE
* */
val ct1Df = customerDf.select($"address_id".cast("long"), $"birthdate".cast("string"))
ct1Df.printSchema
ct1Df.show(false)

/* use of cast(to: DataType): Column */
val ct2Df = customerDf.select($"address_id".cast(LongType), $"birthdate".cast(StringType))
ct2Df.printSchema
ct2Df.show(false)

/* SQL function cast(V as TYPE) in selectExpr */
val ct3Df = customerDf.selectExpr("cast(address_id as string)", "cast(demographics.income_range[0] as double) lowerBound")
ct3Df.printSchema
ct3Df.show(false)

