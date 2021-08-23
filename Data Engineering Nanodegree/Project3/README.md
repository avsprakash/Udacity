# Project 3 - Song Play Analysis for Sparkify


## The music streaming startup - Sparkify, with its growing user base and song database, needs to move thier processes and data onto the cloud.


## Database schema design and ETL pipeline.

Two staging tables are created - STAGING_EVENTS_TABLE and STAGING_SONGS_TABLE to load the complete raw data from the json files stored in the S3 bucket.
Song data: s3://udacity-dend/song_data is loaded in to STAGING_SONGS_TABLE.
Log data: s3://udacity-dend/log_data is loaded in to STAGING_EVENTS_TABLE

This raw data is transformed and loaded in to dimensional tables (SONGPLAY_TABLE, USER_TABLE, SONG_TABLE, ARTIST_TABLE, TIME_TABLE). 
Points to consider while loading data into dimensional tables:
- Use distinct to avoid inserting duplicate rows (used fo USER_TABLE, SONG_TABLE and ARTIST_TABLE).
- load only page='NextSong'.
- Use 'IDENTITY' to autogenerate column values (used while creating SONGPLAY_TABLE).
- Use the same column names as those in the data files, so that we can use "json auto" to load the data easily




## Example queries and results for song play analysis - provided in the word document - 

[Query Analysis.docx](https://github.com/avsprakash/Udacity/blob/9f8629f975731f969060f32dc7f25b6791901185/Data%20Engineering%20Nanodegree/Project3/Query%20Analysis.docx)