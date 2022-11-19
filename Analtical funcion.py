from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName('filter out data').getOrCreate()

    Schema_data = StructType([
     StructField('id', IntegerType()),
     StructField('name', StringType()),
     StructField('gender', StringType()),
     StructField('address', StringType()),
     StructField('salary', DoubleType())
])
df = spark.read.load(r'D:\Pyspark\employee.csv', format='csv', schema=Schema_data)
df.show()
windospec= Window.partitionBy('gender').orderBy('salary')

df.withColumn('row number',row_number().over(windospec)).show()
df.withColumn('rank',rank().over(windospec)).show()
df.withColumn('dense_rank',rank().over(windospec)).show()

df.withColumn("lag",lag('salary',2)).over(windospec).show()
df.withColumn("count",count(col('salary',2))).over(windospec).show()

