from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Data Frame Practice').getOrCreate()
print(spark.sparkContext)
df_pyspark = spark.read.option('header','true').csv('test1.csv', inferSchema=True)
df_pyspark.printSchema()

df_pyspark = spark.read.csv('test1.csv', header=True, inferSchema=True)
df_pyspark.printSchema()
print(type(df_pyspark))

print(df_pyspark.columns)
print(df_pyspark.head(2))
print(df_pyspark.select('Name','Experience'))

### Selecting Columns
df_pyspark.select('Name','Experience').show()
df_pyspark['Name','Age'].show()

print(df_pyspark.dtypes)
df_pyspark.describe().show()

### Adding columns
df_pyspark = df_pyspark.withColumn('Experience After 2 Years', df_pyspark['Experience'] + 2)
df_pyspark.show()

### Deleting Columns
df_pyspark = df_pyspark.drop('Experience After 2 Years')
df_pyspark.show()

### Renaming Columns
df_pyspark = df_pyspark.withColumnRenamed('Name','New Name')
df_pyspark.show()

### How to add and drop more than one columns ?

