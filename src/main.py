from pyspark.sql import SparkSession, functions
import sys, re, math, uuid, os
from timeit import default_timer as timer
from cassandra.cluster import Cluster


schema = "ID STRING, Source STRING, TMC STRING, Severity STRING, Start_Time STRING, End_Time STRING, Start_Lat STRING, " \
         "Start_Lng STRING, End_Lat STRING, End_Lng STRING, Distance(mi) STRING, Description STRING, Number STRING, " \
         "Street STRING, Side STRING, City STRING, County STRING, State STRING, Zipcode STRING, Country STRING, " \
         "Timezone STRING, Airport_Code STRING, Weather_Timestamp STRING, Temperature(F) STRING, Wind_Chill(F) STRING, " \
         "Humidity(%) STRING, Pressure(in) STRING, Visibility(mi) STRING, Wind_Direction STRING, Wind_Speed(mph) STRING, " \
         "Precipitation(in) STRING, Weather_Condition STRING, Amenity STRING, Bump STRING, Crossing STRING, Give_Way STRING, " \
         "Junction STRING, No_Exit STRING, Railway STRING, Roundabout STRING, Station STRING,Stop STRING, Traffic_Calming STRING, " \
         "Traffic_Signal STRING, Turning_Loop STRING, Sunrise_Sunset STRING,Civil_Twilight STRING,Nautical_Twilight STRING, " \
         "Astronomical_Twilight STRING"


def main(keyspace_name, input_file):
    session = Cluster(cluster_seeds).connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace_name+" WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}")
    session.set_keyspace(keyspace_name)
    session.shutdown()
    df = spark.read.csv(input_file, sep=",", header=True)
    # print(df.printSchema())

    df3 = df.select("Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
              "Weather_Condition", "City", "County", "State", "Start_Time", "Sunrise_Sunset")
    print(df3.show(10))

    # most number of accidents occur given city, weather_condition,sunrise_sunset.
    city_df = df3.groupBy("City", "Weather_Condition", "Sunrise_Sunset").agg(functions.count(functions.lit(1)).alias("num_accidents")).sort("num_accidents", ascending=False)
    print(city_df.show(10))

    # most number of accidents occur given city, weather_condition, sunrise_sunset, time_hourly
    # first convert the "Start_Time" to timestamp
    df3 = df3.withColumn("Start_Time", functions.to_timestamp("Start_Time"))
    # print(df3.printSchema())
    df3 = df3.withColumn("hour", functions.hour((functions.round(functions.unix_timestamp("Start_Time")/3600)*3600).cast("timestamp")))
    # print(df3.show())

    city_df = df3.groupBy("City", "Weather_Condition", "Sunrise_Sunset", "hour").agg(functions.count(functions.lit(1)).alias("num_accidents")).sort("num_accidents", ascending=False)
    print(city_df.show())












if __name__=="__main__":
    keyspace_name = sys.argv[1]
    input_file = sys.argv[2]
    # orderkeys = sys.argv[3:]

    cluster_seeds = ['199.60.17.103']

    spark = SparkSession.builder.appName("i dont know").config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(keyspace_name, input_file)
    print("Execution time: {}".format(timer() - st))

