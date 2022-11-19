from pyspark.sql import SparkSession
if __name__=='__main__':
    spark = SparkSession.builder.master("local[*]").appName('empty dataframe').getOrCreate()

header = (id,'name', no,'gender')
input_df=spark.createDataFrame((1,'Ram',25,20000),(2,'Sham',29,30000),(3,'Kiran',31,10000),(4,'Mina',34,14000))
