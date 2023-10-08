import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

val empDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/employees.csv")
empDF.createOrReplaceTempView("emp")
val dptDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/departments.csv")
dptDF.createOrReplaceTempView("dept")

/*
+-------------+---------------+-------------+
|department_id|department_name|     location|
+-------------+---------------+-------------+
|          101|             HR|     New York|
|          102|        Finance|      Chicago|
|          103|    Engineering|San Francisco|
|          104|          Sales|  Los Angeles|
|          105|      Marketing|       Boston|
+-------------+---------------+-------------+


scala> empDF.show
+-----------+----------+---------+-------------+------+
|employee_id|first_name|last_name|department_id|salary|
+-----------+----------+---------+-------------+------+
|          1|      John|      Doe|          101| 60000|
|          2|      Jane|    Smith|          102| 55000|
|          3|   Michael|  Johnson|          101| 62000|
|          4|     Emily| Williams|          103| 58000|
|          5|   William|    Brown|          102| 59000|
|          6|     Susan|    Davis|          103| 63000|
|          7|     James|   Miller|          101| 61000|
|          8|     Linda| Anderson|          102| 57000|
|          9|     David|   Wilson|          103| 64000|
|         10|     Sarah|    Moore|          101| 63000|
|         11|  Laurence|    Smith|         NULL|  NULL|
+-----------+----------+---------+-------------+------+
 */

/* INNER  Join */
// SQL
spark.sql(
  """
    | SELECT emp.first_name, emp.last_name, dept.department_name
    | from emp INNER JOIN dept
    | ON emp.department_id = dept.department_id
    |""".stripMargin).show

/* LEFT  Join, include employees without departments */
// SQL
spark.sql(
  """
    | SELECT emp.first_name, emp.last_name, dept.department_name
    | from emp LEFT OUTER JOIN dept
    | ON emp.department_id = dept.department_id
    |""".stripMargin).show

/* RIGHT  Join, include depts without employees */
// SQL
spark.sql(
  """
    | SELECT emp.first_name, emp.last_name, dept.department_name
    | from emp RIGHT OUTER JOIN dept
    | ON emp.department_id = dept.department_id
    |""".stripMargin).show
/* FULL (OUTER)  Join, include employees with no dept and depts without employees */
// SQL
spark.sql(
  """
    | SELECT emp.first_name, emp.last_name, dept.department_name
    | from emp FULL OUTER JOIN dept
    | ON emp.department_id = dept.department_id
    |""".stripMargin).show

/* CROSS Join (Cartesian Product), include combinations of employees and depts
NOTE no ON expression, otherwise semantics will be invalid
*/
// SQL
spark.sql(
  """
    | SELECT emp.first_name, emp.last_name, dept.department_name
    | from emp CROSS JOIN dept
    |""".stripMargin).show

/* SEMI  TODO Join  returns values from the left side of the relation that has a match with the right */

spark.sql(
  """
    | SELECT  emp.department_id
    | from emp SEMI JOIN dept
    | ON emp.department_id = dept.department_id
    | ORDER BY emp.department_id
    |""".stripMargin).show
/*
+-------------+
|department_id|
+-------------+
|          101|
|          101|
|          101|
|          101|
|          102|
|          102|
|          102|
|          103|
|          103|
|          103|
+-------------+
 */

spark.sql(
  """
    | SELECT  dept.department_id
    | from dept SEMI JOIN emp
    | ON dept.department_id = emp.department_id
    | ORDER BY dept.department_id
    |""".stripMargin).show
/*
+-------------+
|department_id|
+-------------+
|          101|
|          102|
|          103|
+-------------+
 */
/* ANTI  Join  TODO returns values from the left relation that has no match with the right
* */
spark.sql(
  """
    | SELECT emp.employee_id
    | from emp ANTI JOIN dept
    | ON emp.department_id = dept.department_id
    | ORDER BY emp.employee_id
    |""".stripMargin).show
/*
+-----------+
|employee_id|
+-----------+
|         11|
+-----------+
 */

spark.sql(
  """
    | SELECT dept.department_id
    | from dept ANTI JOIN emp
    | ON emp.department_id = dept.department_id
    | ORDER BY dept.department_id
    |""".stripMargin).show
/*
+-------------+
|department_id|
+-------------+
|          104|
|          105|
+-------------+
 */