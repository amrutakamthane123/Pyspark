from pyspark.sql import *
from pyspark.sql.functions import col

if __name__=='__main__':
    Spark= SparkSession.builder.master("local[*]").appName('column class').getOrCreate()

input_data=[Row(id=1, name='ram', address=Row(city='Pune',state='mh')),
            Row(id=1, name='sham', address=Row(city='nagar',state='mh'))]
df = Spark.createDataFrame(input_data)
df.show()
df.select('id').show()
df.select(df.address.city).show()
df.select(df['address.city']).show()
df.select(col('address.city')).show()
df.select(col('address.*')).show()

