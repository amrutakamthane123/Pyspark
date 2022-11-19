

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName('filter out data').getOrCreate()

    Schema_data = StructType([
        StructField('id', IntegerType()),
        StructField('name', StringType()),
        StructField('gender', StringType()),
        StructField('address', StringType()),
        StructField('salary', DoubleType())
    ])

    df = spark.read.load(r'D:\Pyspark\venv\emp.csv', format='csv', schema=Schema_data)
    #df.printSchema()
    df.show()
    df.select(avg('salary')).show()
    df.select(max('salary')).show()
    df.select(min('salary')).show()
    df.select(count('salary')).show()

    
