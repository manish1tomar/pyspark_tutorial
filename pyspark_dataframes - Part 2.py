from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('Practice Dataframes').getOrCreate()
df_pyspark = spark.read.csv('test1.csv', header=True, inferSchema=True)
df_pyspark.show()

### Drop Columns
#df_pyspark = df_pyspark.drop('Name','Experience')
#df_pyspark.show()

### Drop NaN and NULL Rows
df_pyspark.na.drop(how='any').show()
df_pyspark.na.drop(how='all').show()
### Drop those rows where we do not have atleast 2 non NULL/non NaN values are there
df_pyspark.na.drop(thresh=2).show()
### Drop those rows where we do not have atleast 2 non NULL/non NaN values are there
df_pyspark.na.drop(thresh=3).show()
### Drop with subset - delete NULL/NaN values from Experience column only
df_pyspark.na.drop(subset=['Experience']).show()

### Filling missing values
#df_pyspark.na.fill('Missing Value',['Experience','Age']).show()
df_pyspark.na.fill(value='Missing Values').show()

### Fill Missing values by Mean,Median,Mode. We will use Imputer Function
imputer = Imputer(
    inputCols=['Age','Experience','Salary'],
    outputCols=["{}_imputed".format(c) for c in ['Age','Experience','Salary']]
).setStrategy('mean')

imputer.fit(df_pyspark).transform(df_pyspark).show()

imputer = Imputer(
    inputCols=['Age','Experience','Salary'],
    outputCols=["{}_imputed".format(c) for c in ['Age','Experience','Salary']]
).setStrategy('median')

imputer.fit(df_pyspark).transform(df_pyspark).show()

imputer = Imputer(
    inputCols=['Age','Experience','Salary'],
    outputCols=["{}_imputed".format(c) for c in ['Age','Experience','Salary']]
).setStrategy('mode')

imputer.fit(df_pyspark).transform(df_pyspark).show()
