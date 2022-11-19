from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql.functions import col,lit

if __name__=='__main__':
    Spark= SparkSession.builder.master('local[*]').appName('with column').getOrCreate()

    Schema_data= StructType([
        StructField('id', IntegerType()),
        StructField('name',StringType()),
        StructField('gender', StringType()),
        StructField('address',StringType()),
        StructField('salary',DoubleType())])

    df=Spark.read.load(r'D:\Pyspark\employee.csv',format='csv',schema=Schema_data)
    df.printSchema()
    df.show()

    #change datatype of salary column from double to inttype
    #df1 = df.withColumn('salary',col('salary').cast('Integer'))
    #df1.show()
    #df1.printSchema()

    #df.withColumn('salary',col('salary')*100).show()

#asignment
#add new column 'state' which will have static value as 'maharstha'/'mh'

#df.withColumn('State', lit('maharashtra')).show()


#rename column

df.withColumnRenamed('name', 'First_name').printSchema()

#df.drop("city").printSchema()



