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
    print(weather_df.show())
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

if __name__=="__main__":

    spark = SparkSession.builder.appName('US ACCIDENTS ANALYSIS').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    st = timer()
    main()
    print("Execution time: {}".format(timer() - st))


