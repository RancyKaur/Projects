#Script to store aggregate bookings

#import modules

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create environment variables
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"]="/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

# Create spark session
spark=SparkSession.builder.appName("datewise_bookings_aggregates_spark").master("local").getOrCreate()
spark

# Read cab rides data
df=spark.read.csv("/user/root/cab_rides/part-m-00000")

# Check count of data
df.count()

# Check first 10 rows to see what dow we have
df.show(10)

# Check data frame metadata
df.printSchema()

# Rename columns for better understanding and create new dataframe with these columns

new_col = ["booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat","drop_lon","pickup_timestamp","drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver","rating_by_customer","passenger_count"]
new_df = df.toDF(*new_col)

#validate new dataframe
new_df.show(truncate=False)

#Now convert the pickup_timestamp to date by extracting date from pickup_timestamp for aggregation
new_df=new_df.select("booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat","drop_lon",to_date(col('pickup_timestamp')).alias('pickup_date').cast("date"),"drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver","rating_by_customer","passenger_count")

new_df.show()

# create aggregate on pickup_date field
agg_df=new_df.groupBy("pickup_date").count().orderBy("pickup_date")
agg_df.show(5)

#Write the aggregate csv to hadoop
agg_df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/datewise_bookings_agg',header='true')