import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')


os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS_ACCESS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS_ACCESS','AWS_SECRET_ACCESS_KEY')


"""Create spark session with hadoop-aws package.
*
* Using the library to connect to S3.
*
* Outputs:
* - spark: Returns the created spark session.
"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


"""ETL function that processes song data files.
*
* In which it will extract song and artist data, transform this data and save it, creating a table for the songs and another table for the artists.
*
* Parameters:
* - spark: Connection cursor.
* - input_data: Input file path.
* - output_data: Output file path.
"""
def process_song_data(spark, input_data, output_data):
    # Get filepath to song data file.
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # Read song data file.
    print("\n--- Reading song data from json files. --- " + datetime.now().strftime("%Y-%m-%d"))
    df = spark.read.json(song_data)

    #print("Number of records: " + str(df.count()))
    print("- Shows the scheme and some song data records...")
    df.printSchema()
    df.show(5)

    # Extract columns to create songs table.
    print("\n--- Extraction of columns to create songs table. ---")
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration") \
        .orderBy("song_id") \
        .drop_duplicates()

    print("- Shows the scheme and some songs table records...")
    songs_table.printSchema()
    songs_table.show(5)

    # Write songs table to parquet files partitioned by year and artist.
    print("\n--- Writing songs table to parquet files. ---")
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"), mode="overwrite")

    # Extract columns to create artists table.
    print("\n--- Extraction of columns to create artists table. ---")
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude") \
        .orderBy("artist_id") \
        .drop_duplicates()

    print("- Shows the scheme and some artists table records...")
    artists_table.printSchema()
    artists_table.show(5)

    # Write artists table to parquet files.
    print("\n--- Writing artists table to parquet files. ---")
    artists_table.write.parquet(os.path.join(output_data, "artists"), mode="overwrite")


"""ETL function that processes log data files.
*
* In which it will extract users(artist), time table and songplays data, transform this data and save it, creating a table for the users(artist), other table for time table and another table for the songplays.
*
* Parameters:
* - spark: Connection cursor.
* - input_data: Input file path.
* - output_data: Output file path.
"""
def process_log_data(spark, input_data, output_data):
    # Get filepath to log data file.
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # Read log data file.
    print("\n--- Reading log data from json files. --- " + datetime.now().strftime("%Y-%m-%d"))
    df = spark.read.json(log_data)

    print("- Shows the scheme and some log data records...")
    df.printSchema()
    df.show(5)

    # Filter by actions for song plays.
    df = df.where("page = 'NextSong'")

    # Extract columns for users table.
    print("\n--- Extraction of columns to create users table. ---")
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level") \
        .filter("user_id <> ''") \
        .orderBy("user_id") \
        .drop_duplicates()

    print("- Shows the scheme and some users table records...")
    users_table.printSchema()
    users_table.show(5)

    # Write users table to parquet files.
    print("\n--- Writing users table to parquet files. ---")
    users_table.write.parquet(os.path.join(output_data, "users"), mode="overwrite")

    # Create timestamp column from original timestamp column.
    print("\n--- Creation of timestamp and datetime columns. ---")
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("timestamp", get_timestamp("ts"))

    # Create datetime column from original timestamp column.
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime("%Y-%m-%d"))
    df = df.withColumn("datetime", get_datetime("ts"))
    df.show(5)

    # Extract columns to create time table.
    print("\n--- Extraction of columns to create time table. ---")
    time_table = df.select(col("timestamp").alias("start_time"), hour("timestamp").alias("hour"), dayofmonth("timestamp").alias("day"), weekofyear("timestamp").alias("week"), month("timestamp").alias("month"), year("timestamp").alias("year"), date_format("timestamp", "u").alias("weekday")) \
        .orderBy("start_time") \
        .drop_duplicates()

    print("- Shows the scheme and some time table records...")
    time_table.printSchema()
    time_table.show(5)

    # Write time table to parquet files partitioned by year and month.
    print("\n--- Writing time table to parquet files. ---")
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"))

    # Read in song data to use for songplays table.
    print("\n--- Reading song data from json files. ---")
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json")).select("song_id", "title", "artist_id", "artist_name", "year", "duration").drop_duplicates()

    # Extract columns from joined song and log datasets to create songplays table.
    print("\n--- Extraction of columns to create songplays table. ---")
    songplays_table = df.join(song_df, \
        (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration) & (year(df.timestamp) == song_df.year), "left_outer") \
        .select(df.timestamp.alias("start_time"), df.userId.alias("user_id"), df.level, song_df.song_id, song_df.artist_id, df.sessionId.alias("session_id"), df.location, df.userAgent.alias("user_agent"), year(df.timestamp).alias("year"), month(df.timestamp).alias("month")) \
        .orderBy("start_time", "user_id") \
        .withColumn("songplay_id", F.monotonically_increasing_id())

    print("- Shows the scheme and some songplays table records...")
    songplays_table.printSchema()
    songplays_table.show(5)

    # Write songplays table to parquet files partitioned by year and month.
    print("\n--- Writing songplays table to parquet files. ---")
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays"))


"""Main process.
*
* - Creation of the spark session.
* - The data source is defined in an S3 bucket.
* - The data destination is defined in an S3 bucket.
* - Processing of song data.
* - Processing of log data.
"""
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-project-abhaycl/"

    process_song_data(spark, input_data, output_data)
    print("\n-----!!! End of processing of song data files. !!!-----")
    process_log_data(spark, input_data, output_data)
    print("\n-----!!! End of processing of log data files. !!!-----")


"""Defines the name space in which it is running."""
if __name__ == "__main__":
    main()