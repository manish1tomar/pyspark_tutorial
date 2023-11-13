from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practice Aggregation in PySpark').getOrCreate()
df_pyspark = spark.read.csv('test2.csv', header=True, inferSchema=True)
df_pyspark.show()
df_pyspark.printSchema()

### Groupby Operation
df_pyspark.groupby('Name').sum().show()

### Groupby Department giving max salary
df_pyspark.groupby('Department').max().select(['Department','max(Salary)']).show()
df_pyspark.groupby('Department').sum().select(['Department','sum(Salary)']).show()

### No of employees in each department
df_pyspark.groupby('Department').count().show()

### Aggregate without groupby way
df_pyspark.agg({'Salary':'sum'}).show()

### Check the max Salary and show only the top row
df_pyspark.groupby('Name').max('Salary').show(1)

### We can use avg, min, etc