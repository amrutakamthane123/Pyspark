from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StringType,StructField,IntegerType
if __name__=='__main__':
    spark=SparkSession.builder.master("local[*]").appName('empty_rdd').getOrCreate()
#created empty rdd first
    input_rdd=spark.sparkContext.emptyRDD()
    print(f"number of partition :{input_rdd.getNumPartitions()}")

    schema_data=StructType([StructField("id",IntegerType(),False),
                            StructField("name",StringType())])

    empty_df=spark.createDataFrame(input_rdd,schema_data)
    empty_df.show()
    empty_df1=input_rdd.toDF(schema_data)
    empty_df1.show()


