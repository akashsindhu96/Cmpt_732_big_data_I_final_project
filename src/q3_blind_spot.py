from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType
import sys, re, math, uuid, os
from timeit import default_timer as timer
from cassandra.cluster import Cluster
from math import radians, cos, sin, asin, sqrt


@functions.udf(returnType=StringType())
def distance(lat, long):
    points = [] # lst for [(lat, long)]
    threshold = 10  # threshold means how long you want to see the distance b/w accidents eg. 10 equlidean distance units
    blind_spot = [] # lst for [((lat1, long1), (lat2, long2), (lat3, long3))], looking for 3 accidents only
    for i, j in zip(lat, long):
        points.append((i,j))
    print(points)
    i = 0
    while(i < len(points) - 2):
        lat1 = points[i][0]
        long1 = points[i][1]
        lat2 = points[i+1][0]
        long2 = points[i+1][1]
        lat3 = points[i + 2][0]
        long3 = points[i + 2][1]
        print(lat1)

        # for points 1 and 2
        # Transform to radians
        longit_a, latit_a, longit_b, latit_b = map(radians, [float(long1), float(lat1), float(long2), float(lat2)]) # between 1 and 2
        dist_longit = longit_b - longit_a
        dist_latit = latit_b - latit_a

        # Calculate area
        area = sin(dist_latit / 2) ** 2 + cos(latit_a) * cos(latit_b) * sin(dist_longit / 2) ** 2
        # Calculate the central angle
        central_angle = 2 * asin(sqrt(area))
        radius = 6371
        # Calculate Distance
        distance = central_angle * radius
        abs_distance = abs(round(distance, 2))

        # for points 2 and 3
        longit_b, latit_b, longit_c, latit_c = map(radians, [float(long2), float(lat2), float(long3), float(lat3)]) # between 2 and 3, because there is more probability to hit 2 than 1
        dist_longit_2 = longit_c - longit_b
        dist_latit_2 = latit_c - latit_b

        # Calculate area
        area_2 = sin(dist_latit_2 / 2) ** 2 + cos(latit_b) * cos(latit_c) * sin(dist_longit_2 / 2) ** 2
        # Calculate the central angle
        central_angle_2 = 2 * asin(sqrt(area_2))
        # radius = 6371
        # Calculate Distance
        distance_2 = central_angle_2 * radius
        abs_distance_2 = abs(round(distance_2, 2))

        if abs_distance < threshold and abs_distance_2 < threshold:
            blind_spot.append(((lat1, long1), (lat2, long2), (lat3, long3)))

        i += 3 # looking for only 3 accidents for blind spot

    return str(blind_spot)


def main(input_file):
    df = spark.read.csv(input_file, sep=",", header=True)
    df3 = df.select("Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
              "Weather_Condition", "City", "County", "State", "Start_Time", "Sunrise_Sunset").cache()

    # most number of accidents occur given city, weather_condition,sunrise_sunset.
    city_df = df3.groupBy("City", "Weather_Condition", "Sunrise_Sunset").agg(functions.count(functions.lit(1)).alias("num_accidents")).sort("num_accidents", ascending=False)
    print("Most number of accidents occur in a given city, specific weather conditions and sunrise or sunset.")
    city_df.write.format("com.databricks.spark.csv").save('output_city')

    # most number of accidents occur given city, weather_condition, sunrise_sunset, time_hourly
    # first convert the "Start_Time" to timestamp
    df3 = df3.withColumn("Start_Time", functions.to_timestamp("Start_Time"))
    df3 = df3.withColumn("hour", functions.hour((functions.round(functions.unix_timestamp("Start_Time")/3600)*3600).cast("timestamp")))
    city_df = df3.groupBy("City", "Weather_Condition", "Sunrise_Sunset", "hour").agg(functions.count(functions.lit(1)).alias("num_accidents")).sort("num_accidents", ascending=False)
    print("Most number of accidents occur in a given city, in a specific weather conditions, sunrise or sunset, and which hour of the day.")
    city_df.write.format("com.databricks.spark.csv").save('output_city_hr')

    """
    Features: City, Street, no. of accidents, Latitude, Longitude, lat's and long's which are close to eachother.
    
    Which top 10 street and city has most number of accidents in US, given the latitude and longitude we can find out the distance between 
    two accidents. If they are closer to each other compared to a threshold distance then we can find out their exact locations. 
    Here we are considering if within a threshold distance 3 accidents happened, that location is either a blind spot or accident prone. 
    """

    df7 = df.select("City", "Street", "Start_Lat", "Start_Lng")
    street_df = df7.groupBy("City", 'Street').agg(functions.count(functions.lit(1)).alias("no_of_accidents"),
                                                  functions.collect_list('Start_Lat').alias("Latitude_vector"),
                                                  functions.collect_list('Start_Lng').alias("Longitude_vector")).sort("no_of_accidents", ascending=False).limit(100).cache()

    blind_df = street_df.withColumn("blind_spots", distance(street_df.Latitude_vector, street_df.Longitude_vector)).drop("Latitude_vector", "Longitude_vector")
    blind_df.write.format("com.databricks.spark.csv").save('output_accident_prone')



if __name__=="__main__":
    input_file = sys.argv[1]
    spark = SparkSession.builder.appName("q3 and q7").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert spark.version >= '2.4'
    sc = spark.sparkContext
    st = timer()
    main(input_file)
    print("Execution time: {}".format(timer() - st))

