import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, col, monotonically_increasing_id, dayofweek
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
    Description: This function is responsible for Process and load JSON song/artist data from S3 bucket and creates           temporary view
    
    Arguments:
        spark: spark object
        input_data: input dir
        output_data: output dir
    
    Returns:
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    print("Load Song Data File")
    '''
    For Testing
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    '''
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    print(df.show(5))

    # extract columns to create songs table
    print("Song Table")
    print('-' * 40)
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table.dropDuplicates()
    songs_table.createOrReplaceTempView('songs')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'),
                                                               'overwrite')

    # extract columns to create artists table
    print("Artist Table")
    print('-' * 40)
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table.dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')

"""
    Description: This function is responsible for Process and load load log data from S3 bucket and creates temporary         view 
    
    Arguments:
        spark: spark object
        input_data: input dir
        output_data: output dir
    
    Returns:
"""
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print("Load Log Data")
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    print("User Table")
    print('-' * 40)
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    users_table.dropDuplicates()
    users_table.createOrReplaceTempView('users')
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(col("ts")))
    print("Time Table")
    print('-' * 40)
    # extract columns to create time table
    time_table = df.select(
    col('timestamp').alias('start_time'),
    hour('timestamp').alias('hour'),
    dayofmonth('timestamp').alias('day'),
    weekofyear('timestamp').alias('week'),
    month('timestamp').alias('month'),
    year('timestamp').alias('year'),
    date_format('timestamp', 'u').alias('weekday')).orderBy("start_time").drop_duplicates()
    time_table.createOrReplaceTempView("time_table")

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,
                                                                       'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    print("songplay Table")
    print('-' * 40)
    '''
    For Testing
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*.json')
    
    '''
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    
    songplays_df = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title), "left")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplays_df.select(
        df.timestamp.alias("start_time"),
        df.userId.alias("user_id"),
        df.level,
        song_df.song_id,
        song_df.artist_id,
        df.sessionId.alias("session_id"),
        df.location,
        df.userAgent.alias("user_agent"),
        year(df.timestamp).alias('year'),
        month(df.timestamp).alias('month')).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'),
                                                               'overwrite')


def main():
    print("create spark session")
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    print("Song Data")
    print('-' * 40)
    #process_song_data(spark, input_data, output_data)
    print("Log Data")
    print('-' * 40)
    process_log_data(spark, input_data, output_data)
    print('-' * 40)
    print("All Done")


if __name__ == "__main__":
    main()
