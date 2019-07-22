import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads songs data from S3 location given in input_data and writes out songs and artists table to the S3 location pointed to by output_data
    :param spark - contains an instance of SparkSession 
    :param input_data - string - location on S3 of songs files
    :param output_data - string - location on S3 to copy songs and artists tables 
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json') 
    print('Reading song data files from: {}'.format(song_data))
    
    # read song data file
    dfsongs = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = dfsongs.select(dfsongs.song_id, dfsongs.title, dfsongs.artist_id, dfsongs.year, dfsongs.duration)
    # write songs table to parquet files partitioned by year and artist
    output_path = os.path.join(output_data, 'songs_table')
    print('Writing song table to: {}'.format(output_path))
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_path, mode='overwrite')

    # extract columns to create artists table
    artists_table = dfsongs.select(dfsongs.artist_id, (dfsongs.artist_name).alias("name"), (dfsongs.artist_location).alias("location"), (dfsongs.artist_latitude).alias("latitude"), (dfsongs.artist_longitude).alias("longitude"))
    
    # write artists table to parquet files
    output_path = os.path.join(output_data, 'artists_table')
    print('Writing artists table to: {}'.format(output_path))
    artists_table.write.parquet(output_path, mode='overwrite')

    
def process_log_data(spark, input_data, output_data):
    """
    Reads logs data from S3 location given in input_data and writes out users, time and songplays tables to the S3 location pointed to by output_data
    :param spark - contains an instance of SparkSession 
    :param input_data - string - location on S3 of logs  
    :param output_data - string - location on S3 to copy users, time and songplays tables 
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')
    print('Reading log data files from: {}'.format(log_data))

    # read log data file
    dflogs = spark.read.json(log_data)
    
    # filter by actions for song plays
    dflogs = dflogs.filter(dflogs['page']=='NextSong')

    # extract columns for users table    
    users_table = dflogs.select((dflogs.userId).alias("user_id"), (dflogs.firstName).alias("first_name"), (dflogs.lastName).alias("last_name"),dflogs.gender, dflogs.level)

    # write users table to parquet files
    output_path = os.path.join(output_data, 'users_table')
    print('Writing users table to: {}'.format(output_path))
    users_table.write.parquet(output_path, mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts/1000)
    dflogs = dflogs.withColumn('timestamp', get_timestamp(dflogs['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts), TimestampType())
    dflogs = dflogs.withColumn('start_time', get_datetime(dflogs['timestamp']))
        
    # extract columns to create time table
    time_table = dflogs.select(dflogs.start_time, hour(dflogs.start_time).alias('hour'), dayofmonth(dflogs.start_time).alias('day'), weekofyear(dflogs.start_time).alias('week'), month(dflogs.start_time).alias('month'), year(dflogs.start_time).alias('year'), date_format(dflogs.start_time, 'EEEE').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'time_table')
    print('Writing time table to: {}'.format(output_path))
    time_table.write.partitionBy('year', 'month').parquet(output_path, mode='overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json') 
    dfsongs = spark.read.json(song_data)
    
    # extract columns from joined song and log datasets to create songplays table 
    dfsongplays = dflogs.join(dfsongs, [dflogs.song == dfsongs.title, dflogs.artist == dfsongs.artist_name], 'left_outer').select(dflogs.start_time, (dflogs.userId).alias("user_id"), dflogs.level, dfsongs.song_id, dfsongs.artist_id, (dflogs.sessionId).alias("session_id"), dflogs.location, (dflogs.userAgent).alias("user_agent"), year(dflogs.start_time).alias('year'), month(dflogs.start_time).alias('month'))
    songplays_table = dfsongplays.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'songplays_table')
    print('Writing songplays table to: {}'.format(output_path))
    songplays_table.write.partitionBy('year', 'month').parquet(output_path, mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"      
    output_data = "s3a://udacity-dend-project5/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

