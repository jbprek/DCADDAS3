import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


val isoList = List(
  ("2021-12-01", "2021-01-01 12:33:58")
)
val iso = spark.createDataFrame(isoList).toDF("dt", "dtm")



/* Convert ISO compliant Strings to Date, Timestamp  */
//
val df = iso.select($"dt".cast("Date"), $"dtm".cast("TIMESTAMP"))
// OR
val df = iso.select($"dt".cast(DateType), $"dtm".cast(TimestampType))
// OR
val df = iso.selectExpr("to_date(dt) as dt", "to_timestamp(dtm) as dtm")
// OR USE to_date and to_timestamp
val df = iso.select(to_date($"dt").as("dt"), to_timestamp($"dtm").as("dtm"))
df.printSchema
df.show

// Dummy single row empty DF
val lst = List((1))
val emptyDF = lst.toDF.drop("value")


/*
   CURRENT DATE/TIMES
 */
/* Current Date curdate() OR  current_date()*/
emptyDF.select(curdate().as("now")).show
// OR
emptyDF.select(current_date()).show

/* Current Timestamp now() OR current_timestamp() */
emptyDF.select(now().as("now")).show(false)
// OR
emptyDF.select(current_timestamp()).show(false)


/* Convert format Dates to String
 - Use of date_format
  NOTE resulting type is String
  NOTE there's no timestamp_format
*/
// TODO use expr
df.select(
  date_format($"dt","yyyy-MM-dd").as("dt_iso"),
  date_format($"dtm","yyyy-MM-dd H:m:s").as("dtm_iso")
).show(false)

df.select(
  date_format($"dt","MM/dd/yyyy").as("dt_us"),
  date_format($"dtm","EEEE MMMM yyyy hh:mm:ss a").as("dtm_us")
).show(false)
/*
+----------+------------------------------+
|12/01/2021|Friday January 2021 12:33:0 PM|
+----------+------------------------------+
 */

// 'M' OR 'L'
spark.sql(sql).show(false)

spark.sql(""" select date_format(date '2013-09-15', "M") """).show(false)
spark.sql(""" select date_format(date '2013-09-15', "L") """).show(false)
//  6
  spark.sql(""" select date_format(date '2013-09-15', "MMMM") """).show(false)
// September


/* Parse extended formats Date */
emptyDF.select(to_date(lit("07/30/2024"), "MM/dd/yyyy").as("dt")).show(false)
/* Parse extended formats Timestamps */

emptyDF.select(to_timestamp(lit("01/07/2001 23 30 15"),"MM/dd/yyyy HH mm ss").as("dtm")).show(false)

// TODO Fix
emptyDF.select(to_timestamp(lit("Friday January 2021 12:33:0 PM"),"EEEE MMMM yyyy HH:mm:ss a").as("dtm")).show(false)

/* Date extract year,month,date, date of year, year week from Date */
df.select(
  $"dtm",
  year($"dtm"),
  month($"dtm"),
  day($"dtm"),
  hour($"dtm"),
  minute($"dtm"),
  second($"dtm")
).show(false)


/* Compare Dates TODO */

/* Compare Timestamps TODO */

/* To Unix EPOCH to_unix_timestamp */
val u1wDF=  emptyDF.select(to_unix_timestamp(now()))
u1wDF.show(false)
u1wDF.printSchema

/* From Unix EPOCH return a Timestamp string */
val u1rDF=  emptyDF.select(from_unixtime(lit(1696676525L)).as("tm"))
u1rDF.show(false)
u1rDF.printSchema
// Convert explicitely to Date
u1rDF.select($"tm".cast("date").as("dt")).show(false)
// Convert explicitely to Timestamp
u1rDF.select($"tm".cast("Timestamp")).show(false)

/* TODO Intervals */