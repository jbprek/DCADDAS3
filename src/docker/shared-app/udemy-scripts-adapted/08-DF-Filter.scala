import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/*
def filter(conditionExpr: String): Dataset[T]
Filters rows using the given SQL expression.
I.e : peopleDs.filter("age > 15")

def filter(condition: Column): Dataset[T]
Filters rows using the given condition
I.e peopleDs.filter($"age" > 15)

# where same args and behavior with filter
 */


// Find by specific attribute
// Find by id
personDF.filter($"id" === 1).show
personDF.where($"id" === 1).show
personDF.filter("id = 1").show
personDF.where("id = 1").show

// Date queries
// All people born a specific date
personDF.where($"dob === '1990-01-01'").show
personDF.where("dob = '1990-01-01'").show
// All people born in the 1st of any month
personDF.where("day(dob) = 1").show
personDF.where(dayofmonth($"dob") === 1).show
//  All people older that 30 years
personDF.where("datediff(current_date,dob) > 30*365").show
personDF.where(datediff( current_date,$"dob") > 30*365).show

personDF.where(year($"dob") > 2000).show
  filter(month($"dob") === 1).
  where("day(dob) > 3").
  select("*").show()

/* Complex condition */
/* AND can be applied by subsequent filter/selecct calls */
// Name = Laura or birth year less than 2000
personDF.where("year(dob) < 2000 or name = 'Laura'").show
personDF.where(year($"dob") < 2000 || $"name" === "Laura").show
personDF.where(year($"dob") < 2000 or $"name" === "Laura").show

/* Filtering null, not null */
personDF.where("isnotnull(dob)").show
personDF.where("isnull(dob)").show
personDF.where($"dob".isNotNull).show
personDF.where($"dob".isNull).show


/* Use of native constants in filters*/
val subsVar = "Laura"
personDF.where(s"year(dob) < 2000 or name = '$subs'").show
personDF.where(year($"dob") < 2000 || $"name" === subsVar).show
personDF.where(year($"dob") < 2000 or $"name" === subsVar).show