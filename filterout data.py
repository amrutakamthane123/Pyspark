from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
    #df.printSchema()
    #df.show()
    #df.filter(df.gender=='male').select('id','name','gender').show()
    df.filter(df.gender != 'male'.select('id', 'name', 'gender').show())
    #salary less than 10000
    #df.filter(df.salary<10000).show()
    #startwith endwith
    #df.filter(df.gender.startswith('M')).show()
    #Git Commment check
