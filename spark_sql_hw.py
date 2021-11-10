from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Create Session').master('local').getOrCreate()

df = spark.read.option('header', True).option('sep',',').option('inferSchema',True).csv('owid-covid-data.csv')

# Вопрос 1
q1 = df.filter(~F.col('iso_code').like('OWID%')) \
    .filter(F.col('date')=='2021-03-31') \
        .withColumn('percentage', F.col('total_cases')/F.col('population')*100)\
            .select('iso_code', 'location', 'percentage') \
                .orderBy('percentage', ascending=False) \
                    .withColumnRenamed('location', 'country').limit(15)
q1.repartition(1).write.csv('/opt/bitnami/q1')

# Вопрос 2
q2 = df.filter(~F.col('iso_code') \
    .like('OWID%')).filter(F.col('date').between('2021-03-24','2021-03-31'))\
        .select('date', 'location', 'new_cases')\
        .orderBy('new_cases', ascending=False)\
            .withColumnRenamed('location', 'country').limit(10)
q2.repartition(1).write.csv('/opt/bitnami/q2')

# Вопрос 3
lag_window = Window.partitionBy('location').orderBy('date')
q3 = df.filter(F.col('location')=='Russia').filter(F.col('date').between('2021-03-24','2021-03-31'))\
    .select('date', 'location', 'new_cases')\
    .withColumn('prev_day_new_cases', F.lag(F.col('new_cases'), 1).over(lag_window))\
    .withColumn('delta', 
          F.col('new_cases') - F.col('prev_day_new_cases'))\
              .select('date', 'location', 'prev_day_new_cases', 'new_cases', 'delta')
q3.repartition(1).write.csv('/opt/bitnami/q3')