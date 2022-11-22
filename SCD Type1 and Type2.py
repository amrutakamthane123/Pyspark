from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
if __name__=='__main__':
    Spark= SparkSession.builder.master("local[*]").appName('scd').getOrCreate()

    #type 1, Created one dataframe using i/p file
    df1=Spark.read.csv(r'D:\Pyspark\scd11.csv',inferSchema=True,header=True)
    df1.show()
    #creating second dataframe using i/p file
    df2=Spark.read.csv(r'D:\Pyspark\scd22.csv',inferSchema=True,header=True)
    df2.show()
    #performing unionall
    df3=df1.unionAll(df2)
    df3.show()
    df4=df3.select('*',dense_rank().over(Window.partitionBy('id').orderBy(df3.start_date.desc())).alias('rank'))
    df4.show()
    df4.filter(col('rank') == 1).select('id', 'name', 'city', 'start_date', 'end_date').show()

    #scd 2
    #created view on union table
    df3.createOrReplaceTempView('view1')
    Spark.sql('select * from view1').show()
    df_1=df3.select('*',lead('start_date',1).over(Window.partitionBy('id').orderBy('start_date')).alias('end_date1'))

    df_1.show()
    df_1.select('id','name','city','start_date',expr("case when end_date1 is null then end_date " + "else end_date1 end").alias('end_date'),
    expr("case when end_date1 is null then 'active' "+ "else 'expired' end").alias('status')).orderBy('start_date','id').show()

