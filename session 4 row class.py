from pyspark.sql import Row
from pyspark.sql import SparkSession

if __name__=='__main__':
    spark = SparkSession.builder.master("local[*]").appName('empty dataframe').getOrCreate()

input_data=[Row(id=1, name='ram', address=Row(city='Pune',state='mh')),
            Row(id=1, name='sham', address=Row(city='nagar',state='mh'))]
df= spark.createDataFrame(input_data)
df.show()
df.printSchema()