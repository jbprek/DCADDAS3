# JDBC Connectivity

## Connect to a mysql database

Given a database:

- host : HOST
- port : PORT
- username : USR
- password : PSWD
- database : DB
- table name : TBL

```scala
val jdbcURL = "jdbc:mysql://address=(host=HOST)(port=PORT)(user=USR)(password=PSWD)/DB"
val df = spark.sqlContext.read.format("jdbc").
  option("url",jdbcURL).
  option("dbTable",TBL).
  option("driver","com.mysql.cj.jdbc.Driver")
```