import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/*
 * Three category of aggregations
 * 1. Simple
 * 2. Grouping
 ** 3. Windowing Aggregations
 */


/* Window Aggregations
*   General strategy
*  1. Identify your partioning columns
*  2. Identify your ordering requirement
*  3. Define your window start and end

* */

import org.apache.spark.sql.expressions.Window

val runningTotalWindow = Window.partitionBy("Country").
  orderBy("WeekNumber").
  rowsBetween(-2, Window.currentRow)

summaryDF.withColumn("RunningTotal",
  sum("InvoiceValue").over(runningTotalWindow)
).show()
