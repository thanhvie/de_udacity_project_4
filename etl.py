"""This is the script for process ETL pipeline."""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a spark session.

    :return spark: Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", config['AWS']['AWS_ACCESS_KEY_ID'])
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", config['AWS']['AWS_SECRET_ACCESS_KEY'])
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process ETL pipeline for songs table and artists table.

    :param spark: spark session
    :param input_data: folder path that contain input data
    :param output_data: folder path that contain output data
    :return: None
    """
    # get filepath to song data file
    input_song_data = input_data + 'song_data/A/B/C/*.json'

    # read song data file
    df = spark.read.json(input_song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    output_songs_data = output_data + 'songs/songs.parquet'
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        output_songs_data, 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude').dropDuplicates()

    column_names = ["artist_id", "name", "location", "latitude", "longitude"]
    artists_table = artists_table.toDF(*column_names)

    # write artists table to parquet files
    output_artists_data = output_data + 'artists/artists.parquet'
    artists_table.write.parquet(output_artists_data, 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process ETL pipeline for users, time and songplays table.

    :param spark: spark session
    :param input_data: folder path that contain input data
    :param output_data: folder path that contain output data
    :return: None
    """
    # get filepath to log data file
    input_log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    df_log_data = spark.read.json(input_log_data)

    # filter by actions for song plays
    df_log_data = df_log_data.where(df_log_data.page == 'NextSong')

    # extract columns for users table
    df_users_table = df_log_data.where(df_log_data.userId.isNotNull())
    df_users_table = df_users_table.select(
        'userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    column_names = ["user_id", "first_name", "last_name", "gender", "level"]
    df_users_table = df_users_table.toDF(*column_names)

    df_users_table = df_users_table.withColumn(
        "user_id", col("user_id").cast('integer'))

    # write users table to parquet files
    output_users_data = output_data + 'users/users.parquet'
    df_users_table.write.parquet(output_users_data, 'overwrite')

    # extract columns to create time table
    df_time_table = df_log_data.where(df_log_data.ts.isNotNull())
    df_time_table = df_time_table.select('ts').dropDuplicates()

    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df_time_table = df_time_table.withColumn(
        'start_time', get_timestamp('ts').cast(TimestampType()))

    # add columns
    df_time_table = df_time_table.withColumn('hour', hour('start_time'))
    df_time_table = df_time_table.withColumn('day', dayofmonth('start_time'))
    df_time_table = df_time_table.withColumn('week', weekofyear('start_time'))
    df_time_table = df_time_table.withColumn('month', month('start_time'))
    df_time_table = df_time_table.withColumn('year', year('start_time'))
    df_time_table = df_time_table.withColumn(
        'weekday', dayofweek('start_time'))

    df_time_table = df_time_table.drop('ts')

    # write time table to parquet files partitioned by year and month
    output_time_data = output_data + 'time/time.parquet'
    df_time_table.write.partitionBy('year', 'month').parquet(
        output_time_data, 'overwrite')

    # read in song data to use for songplays table
    input_song_data = input_data + 'song_data/A/B/C/*.json'
    df_song_data = spark.read.json(input_song_data)

    # extract columns from joined song and log datasets to create songplays table
    df_songplays_table = df_log_data.join(df_song_data,
                                          (df_log_data.song == df_song_data.title) & (
                                              df_log_data.artist == df_song_data.artist_name),
                                          how='inner')

    df_songplays_table = df_songplays_table.where(
        (df_songplays_table.page == 'NextSong') & (df_songplays_table.ts.isNotNull()))
    df_songplays_table = df_songplays_table.withColumn(
        'timestamp', get_timestamp('ts').cast(TimestampType()))

    # Add songplay_id
    df_songplays_table = df_songplays_table.withColumn(
        'songplay_id', monotonically_increasing_id())

    # Add columns
    df_songplays_table = df_songplays_table.withColumn(
        'month', month('timestamp'))
    df_songplays_table = df_songplays_table.withColumn(
        'year', year('timestamp'))

    # Select required columns
    df_songplays_table = df_songplays_table.select('songplay_id', 'timestamp', 'userId', 'level',
                                                   'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', 'month', 'year').dropDuplicates()

    # Rename columns
    column_names = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id',
                    'artist_id', 'session_id', 'location', 'user_agent', 'month', 'year']
    df_songplays_table = df_songplays_table.toDF(*column_names)

    # write songplays table to parquet files partitioned by year and month
    output_songplays_data = output_data + 'songplays/songplays.parquet'
    df_songplays_table.write.partitionBy('year', 'month').parquet(
        output_songplays_data, 'overwrite')


def main():
    """Entry function to execute code."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4-outputs/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
