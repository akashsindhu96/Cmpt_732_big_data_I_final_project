from pyspark.sql import SparkSession, functions,types
from timeit import default_timer as timer
from pyspark.sql.functions import expr

# defining the schema for the dataset - US Accidents
schema = "ID STRING, Source STRING, TMC STRING, Severity STRING, Start_Time STRING, End_Time STRING, Start_Lat STRING, " \
         "Start_Lng STRING, End_Lat STRING, End_Lng STRING, Distance(mi) STRING, Description STRING, Number STRING, " \
         "Street STRING, Side STRING, City STRING, County STRING, State STRING, Zipcode STRING, Country STRING, " \
         "Timezone STRING, Airport_Code STRING, Weather_Timestamp STRING, Temperature(F) STRING, Wind_Chill(F) STRING, " \
         "Humidity(%) STRING, Pressure(in) STRING, Visibility(mi) STRING, Wind_Direction STRING, Wind_Speed(mph) STRING, " \
         "Precipitation(in) STRING, Weather_Condition STRING, Amenity STRING, Bump STRING, Crossing STRING, Give_Way STRING, " \
         "Junction STRING, No_Exit STRING, Railway STRING, Roundabout STRING, Station STRING,Stop STRING, Traffic_Calming STRING, " \
         "Traffic_Signal STRING, Turning_Loop STRING, Sunrise_Sunset STRING,Civil_Twilight STRING,Nautical_Twilight STRING, " \
         "Astronomical_Twilight STRING"

def main():

    input_file = '/Users/bilalhussain/Downloads/US_Accidents_June20.csv'
    df = spark.read.csv(input_file, sep=",", header=True)
    df = df.drop('TMC','End_Lat','End_Lng','Number','Country','Timezone','Airport_Code'
                 ,'Wind_Chill(F)','Wind_Direction')
    print(df.columns)

    # -------- GENERAL ACCIDENT TRENDS ------------

    # GRAPH 1
    # TOTAL ACCIDENT REPORTS(2016 - 2020)
    total_states = df.groupBy('State').agg(functions.count(functions.lit(1)).alias('Total accidents in each state')).sort('Total accidents in each state',ascending=False)
    print(total_states.show())
    total_states.coalesce(1).write.csv('/Users/bilalhussain/Downloads/Total_States.csv', header=True, mode='overwrite')

    # GRAPH 2
    #ACCIDENT BY SEVERITIES (LOW TO HIGH)
    lth= df.groupBy('State','Severity').agg(functions.count(functions.lit(1)).alias('Total Severity')).sort('Severity',ascending=False)
    print(lth.show())
    lth.coalesce(1).write.csv('/Users/bilalhussain/Downloads/Accidents_by_severity.csv', header=True, mode='overwrite')

    # --------- ACCIDENT COUNT (2016 - 2020) ------------

    # GRAPH 3
    # ACCIDENT COUNT PER YEAR
    year_df = df.select('State', (functions.year(functions.to_timestamp("Start_Time"))).alias('year')).groupBy(
        'year').agg(functions.count(functions.lit(1))).sort('year')
    print(year_df.show())
    year_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/Count_by_Year.csv', header=True, mode='overwrite')

    # GRAPH 4
    # ACCIDENT COUNT BY MONTH
    month_df = df.select('State', (functions.month(functions.to_timestamp("Start_Time"))).alias('month')).groupBy(
        'month').agg((functions.count(functions.lit(1))).alias('total_accidents')).sort('month')
    print(month_df.show())
    month_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/Count_by_month.csv', header=True, mode='overwrite')

    # GRAPH 5
    # ACCIDENT COUNT BY DAY OF THE WEEK
    weekday_df = df.select((functions.dayofweek(functions.to_timestamp("Start_Time"))).alias('week_day')).groupBy(
        'week_day').agg((functions.count(functions.lit(1))).alias('total_accidents')).sort('week_day')
    print(weekday_df.show())
    weekday_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/Count_by_weekday.csv', header=True, mode='overwrite')

    # GRAPH 6
    # ACCIDENT COUNT BY HOUR
    hour_df = df.select((functions.hour(functions.to_timestamp("Start_Time"))).alias('hour_df')).groupBy('hour_df')\
        .agg((functions.count(functions.lit(1))).alias('total_accidents')).sort('hour_df')
    print(hour_df.show(50))
    hour_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/Count_by_hour.csv', header=True,mode='overwrite')

    # ----------- ACCIDENT TIMELINES -----------

    # GRAPH 7
    # ACCIDENTS DUE TO TOP 10 WEATHER CONDITIONS
    weather_df = df.filter(df['Weather_Condition'].isNotNull())
    #FINDING THE WEATHER CONDITION WHICH CAUSED HIGHEST NUMBER OF ACCIDENTS
    weather_df = weather_df.groupBy('Weather_Condition').agg(functions.count(functions.lit(1)).alias('top_weather')).sort('top_weather',ascending=False)
    # print(weather_df.show())
    top_10_weather = df.where(expr("Weather_Condition = 'Fair' or Weather_Condition= 'Clear' or Weather_Condition= 'Mostly Cloudy' or Weather_Condition= 'Overcast'"
                                   "or Weather_Condition= 'Partly Cloudy' or Weather_Condition= 'Cloudy' or Weather_Condition= 'Scattered Clouds' or Weather_Condition= 'Light Rain' "
                                   "or Weather_Condition= 'Light Snow' or Weather_Condition= 'Rain'"))
    #REFORMATTING THE DATE IN THIS FORMAT DAY/MONTH FOR PLOTTING PURPOSES
    @functions.udf(returnType=types.StringType())
    def date_reformat(month):
        month = f"1/{month}"
        return month
    top_10_weather = top_10_weather.groupBy(functions.year(functions.to_timestamp("Start_Time")).alias('year'),
                                            functions.month(functions.to_timestamp("Start_Time")).alias('month'),'Weather_Condition')\
        .agg(functions.count(functions.lit(1)).alias('weather_acc')).sort('year','month')

    top_10_weather = top_10_weather.withColumn('month', date_reformat(top_10_weather['month']))
    top_10_weather=top_10_weather.select('Weather_Condition','weather_acc',functions.concat(functions.col('month'),functions.lit("/"),
                                                                                            functions.col('year')).alias('mon_year'))
    print(top_10_weather.show())
    top_10_weather.coalesce(1).write.csv('/Users/bilalhussain/Downloads/by_month_weather.csv', header=True,mode='overwrite')


    # GRAPH 8
    # AGGREGATED MONTHLY ACCIDENTS IN TOP TWENTY CITIES
    import calendar
    #GETTING NAME OF THE MONTH FROM THE MONTH NUMBER
    @functions.udf(returnType=types.StringType())
    def get_month_name(month):
        return calendar.month_name[month]

    #FIND THE CITIES WITH HIGHEST NUMBER OF ACCIDENTS
    top_20_cities = df.groupBy('City').agg(functions.count(functions.lit(1)).alias('Acc Count')).sort('Acc Count',
                                                                                                      ascending=False)
    print(top_20_cities.show())
    top_20_cities = df.where(expr(
        "City = 'Houston' or City= 'Los Angeles' or City= 'Charlotte' or City= 'Dallas'"
        "or City= 'Austin' or City= 'Raleigh' or City= 'Atlanta' or City= 'Oklahoma City' "
        "or City= 'Miami' or City= 'Baton Rouge' or City = 'Nashville' or City= 'Sacramento' or City= 'Orlando' or City= 'Phoenix'"
        "or City= 'Minneapolis' or City= 'Seattle' or City= 'San Diego' or City= 'San Antonio' "
        "or City= 'Richmond' or City= 'San Jose' "))
    top_20_cities = top_20_cities.groupBy(functions.month(functions.to_timestamp("Start_Time")).alias('month'), 'City') \
        .agg(functions.count(functions.lit(1)).alias('total_acc')).sort('month')
    top_20_cities = top_20_cities.withColumn('month', get_month_name(top_20_cities['month']))
    print(top_20_cities.show())
    top_20_cities.coalesce(1).write.csv('/Users/bilalhussain/Downloads/top_20_cities.csv', header=True,mode='overwrite')

    # --------ACCIDENT TRENDS W.R.T VISIBILITY ---------

    # GRAPH 9 AND GRAPH 10
    # LOW SEVERITY ACCIDENTS DUE TO LOW VISIBILITY
    # HIGH SEVERITY ACCIDENTS DUE TO LOW VISIBILITY
    visibility_df = df.filter(df['Visibility(mi)'].isNotNull()).where((df['Visibility(mi)']<=5.0) & ((df['Severity'] =="1") | (df['Severity']=="2")))
    visibility_df= visibility_df.groupBy('State').agg(functions.count(functions.lit(1))
                                                                          .alias('Low Severity Accidents due to Low visibility')).sort('Low Severity Accidents due to Low visibility',ascending=False)
    visibility_df=visibility_df.withColumnRenamed('State','States')
    # print(visibility_df.show())
    high_sev_lowviz = df.filter(df['Visibility(mi)'].isNotNull()).where((df['Visibility(mi)']<=5.0) & ((df['Severity'] =="3") | (df['Severity']=="4")))
    high_sev_lowviz = high_sev_lowviz.groupBy('State').agg(functions.count(functions.lit(1))
                                                                          .alias('High Severity Accidents due to Low visibility')).sort('High Severity Accidents due to Low visibility',ascending=False).cache()
    # print(high_sev_lowviz.show())

    joined_df = visibility_df.join(functions.broadcast(high_sev_lowviz),[visibility_df['States']==high_sev_lowviz['State']])
    joined_df= joined_df.drop('State')
    print(joined_df.show())
    joined_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/l_h_visibility.csv',header=True,mode='overwrite')

    # ----------------ACCIDENT TRENDS IN THE MOST ACCIDENT PRONE CITY----------

    # GRAPH 11
    # NUMBER OF ACCIDENTS IN HOUSTON AT DIFFERENT TIMES OF THE DAY DUE TO DIFFERENT WEATHER CONDITIONS
    df3 = df.select('*')

    # most number of accidents occur given city, weather_condition,sunrise_sunset.


    # most number of accidents occur given city, weather_condition, sunrise_sunset, time_hourly
    # first convert the "Start_Time" to timestamp
    df3 = df3.withColumn("Start_Time", functions.to_timestamp("Start_Time"))
    df3 = df3.withColumn("hour", functions.hour(
        (functions.round(functions.unix_timestamp("Start_Time") / 3600) * 3600).cast("timestamp")))

    # CONVERTING THE HOUR NUMBER TO TIMES OF DAY
    @functions.udf(returnType=types.StringType())
    def get_time_of_day(hour):
        if hour == 5 or hour == 6 or hour == 7 or hour == 8:
            return "Early Morning"
        elif hour == 9 or hour == 10 or hour == 11 or hour == 12:
            return 'Late Morning'
        elif hour == 13 or hour == 14 or hour == 15:
            return 'Early Afternoon'
        elif hour == 16 or hour == 17:
            return 'Late Afternoon'
        elif hour == 18 or hour == 19 or hour == 20:
            return 'Early Evening'
        elif hour == 21 or hour == 22 or hour == 23 or hour == 0 or hour == 1 or hour == 2 or hour == 3 or hour == 4:
            return 'Night'

    city_df = df3.filter(df['Weather_Condition'].isNotNull()).filter(df['City'].isNotNull())
    city_df = city_df.groupBy("City", "Weather_Condition", "hour").agg(
        functions.count(functions.lit(1)).alias("num_accidents")).sort("num_accidents", ascending=False)
    city_df = city_df.withColumn('hour', get_time_of_day(city_df['hour']))
    city_df = city_df.filter(city_df['City'] == 'Houston')
    city_df = city_df.where(expr(
        "Weather_Condition = 'Fair' or Weather_Condition= 'Clear' or Weather_Condition= 'Mostly Cloudy' or Weather_Condition= 'Overcast'"
        "or Weather_Condition= 'Partly Cloudy' or Weather_Condition= 'Cloudy' or Weather_Condition= 'Scattered Clouds' or Weather_Condition= 'Light Rain' "
        "or Weather_Condition= 'Rain' or Weather_Condition= 'Fog' or Weather_Condition= 'Heavy Rain' or Weather_Condition= 'Haze'"
        "or Weather_Condition= 'Shallow Fog' or Weather_Condition= 'Thunder' or Weather_Condition= 'Thunderstorm' or Weather_Condition= 'Fair / Windy'"
        "or Weather_Condition= 'Cloudy / Windy' or Weather_Condition= 'Light Drizzle' or Weather_Condition= 'Partly Cloudy / Windy' or Weather_Condition= 'Mostly Cloudy / Windy'"))
    city_df = city_df.groupBy("City", "Weather_Condition", "hour").agg(
        functions.sum('num_accidents').alias('num_accidents')).sort('num_accidents', ascending=False)

    city_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/day_time.csv', header=True, mode='overwrite')


    #GRAPH 12
    #NUMBER OF ACCIDENTS IN HOUSTON AT DIFFERENT TIMES OF THE DAY WITH DIFFERENT SEVERITIES

    houston_df = df3.filter(df3['City']=='Houston')
    houston_streets = houston_df.groupBy('Street').agg(
        functions.count(functions.lit(1)).alias('total_acc_in_streets')).sort('total_acc_in_streets',
                                                                              ascending=False).limit(20)
    houston_df = houston_df.select('City','Severity','hour')
    houston_df = houston_df.withColumn('hour',get_time_of_day(houston_df['hour']))
    houston_df = houston_df.groupBy('City','Severity','hour').agg(functions.count(functions.lit(1)).alias('total_acc_per_Sev'))
    print(houston_df.show())
    houston_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/houston_sev/', header=True, mode='overwrite')

    #GRAPH 13
    #Top 20 Houston Streets for Accidents
    houston_streets.coalesce(1).write.csv('/Users/bilalhussain/Downloads/houston_streets/', header=True, mode='overwrite')
    print(houston_streets.show(25))


    #GRAPH 14
    houston_features = df3.filter(df3['City'] == 'Houston')
    houston_features = houston_features.select('City','Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal','Turning_Loop')
    amenity= houston_features.filter(houston_features['Amenity']==True).agg(functions.count(functions.lit(1))).collect()[0][0]
    bump =houston_features.filter(houston_features['Bump'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    crossing =houston_features.filter(houston_features['Crossing'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    give_way =houston_features.filter(houston_features['Give_Way'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Junction = houston_features.filter(houston_features['Junction'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    No_Exit = houston_features.filter(houston_features['No_Exit'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Railway = houston_features.filter(houston_features['Railway'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Roundabout = houston_features.filter(houston_features['Roundabout'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Station = houston_features.filter(houston_features['Station'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Stop = houston_features.filter(houston_features['Stop'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Traffic_Calming = houston_features.filter(houston_features['Traffic_Calming'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Traffic_Signal = houston_features.filter(houston_features['Traffic_Signal'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]
    Turning_Loop =houston_features.filter(houston_features['Turning_Loop'] == True).agg(functions.count(functions.lit(1))).collect()[0][0]


    features_df=spark.createDataFrame(
        [
            ('Amenity', amenity),
            ('Bump', bump),
            ('Crossing', crossing),
            ('Give_Way', give_way),
            ('Junction', Junction),
            ('No_Exit', No_Exit),
            ('Railway', Railway),
            ('Roundabout', Roundabout),
            ('Station', Station),
            ('Stop', Stop),
            ('Traffic_Calming', Traffic_Calming),
            ('Traffic_Signal', Traffic_Signal),
            ('Turning_Loop', Turning_Loop),

        ],
        ['type', 'count']  # add your columns label here
    )
    features_df.coalesce(1).write.csv('/Users/bilalhussain/Downloads/houston_features/', header=True, mode='overwrite')
    print(features_df.show())

if __name__=="__main__":

    spark = SparkSession.builder.appName('US ACCIDENTS ANALYSIS').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    st = timer()
    main()
    print("Execution time: {}".format(timer() - st))


