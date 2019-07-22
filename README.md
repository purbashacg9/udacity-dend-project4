## Project Description

In this project, the goal was to build an ETL pipeline to ingest Sparkify's songs and events data hosted on a data lake on S3, process the data into analytics tables using Spark and create a star schema that can be used for running analytical queries. The analytics  tables will need to be copied back to S3.  

## Datasets

Song Dataset - s3://udacity-dend/song_data  
Each file in in json and contains information about a single song. They are arranged in a folder structure based on the song's tracking id.

Log Dataset - s3://udacity-dend/log_data
The log files in the dataset are partitioned by year and month. They are named as YYYY-MM-DD-events.json. These files contain the data for a song play event. Each record contains the timestamp of the event, the user who is playing the song, session id of the user, location of the user, type of user - paid/free,  song played by the user, artist for the song, length of the song etc. A valid song play event is the one with page=NextSong.

## ETL Process

etl.py does the following -
1. Reads the songs dataset which is a collection of JSON files from S3
2. Creates a Spark SQL dataframe composed of song specific information from the data and saves the data as songs_table on S3. songs_table is a collection of parquet files partitioned by year and artist.  
3. Creates a Spark SQL dataframe composed of artist specific information from the data and saves it as artists_table on S3. artists_table is a collection of parquet files.  
4. Reads log data which is also a collection of JSON Files from S3.
5. Creates a Spark SQL dataframe containing the logs data and generates a user_table, time_table and songplays_table from this dataframe. All three tables are saved to S3 as parquet files. time_table and songplays_table are partitioned by year and month.   

## Project Files

-   dwh.cfg - Contains the AWS credentials for accessing S3 for writing analytical tables.
-   etl.py - contains code for ETL process to load data into Spark tables and write data out to parquet files on S3.

## Running the scripts
Run python etl.py from terminal window.
