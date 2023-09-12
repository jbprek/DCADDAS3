val ds = spark.read.textFile("/data/persons.csv").toDF().as[String]
val result = spark.read.schema(personSchema).option("inferSchema","false").option("header","false").csv(ds)

result.printSchema


val initial = spark.read.option("header", "false").schema(personSchema).csv("/data/persons.csv")
val result = spark.createDataFrame(initial.rdd, personSchema)


val csvRdd =spark.sparkContext.textFile("/data/persons.csv")
val result = spark.createDataFrame(csvRdd, personSchema)