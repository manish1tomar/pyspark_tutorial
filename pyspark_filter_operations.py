from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practice DataFrame Filter operations').getOrCreate()
print(spark.sparkContext)

df_pyspark = spark.read.csv('test1.csv', header=True, inferSchema=True)
df_pyspark.show()

### Filter Operations
df_pyspark.filter('Salary<=20000').show()
df_pyspark.filter('Salary<=20000').select(['Name','Age']).show()
df_pyspark.filter(df_pyspark['Salary'] <= 20000).show()
df_pyspark.filter( (df_pyspark['Salary'] <= 20000) & (df_pyspark['Salary'] >= 50000) ).show()
df_pyspark.filter( (df_pyspark['Salary'] <= 20000) | (df_pyspark['Salary'] >= 50000) ).show()