import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Drop using column name strings, No failure on unkown columns
customerDf.drop("address_id", "birth_country", "lkajkdjf").columns

// Drop using Column Objects, No failure on unkown column names
customerDf.drop($"lastname", $"firstname", $"lkajkdjf").columns

// SOS use either string or column objects
customerDf.drop($"lastname", "firstname").columns