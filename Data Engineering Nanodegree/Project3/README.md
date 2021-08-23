# Project 3 - Song Play Analysis for Sparkify


## Project Description

The music streaming startup - Sparkify, with its growing user base and song database, needs to move thier processes 
and data onto the cloud.


## Database schema design and ETL pipeline.

Two staging tables are created - STAGING_EVENTS_TABLE and STAGING_SONGS_TABLE to load the complete raw data from the json 
files stored in the S3 bucket.

Song data: s3://udacity-dend/song_data is loaded in to STAGING_SONGS_TABLE.  

Log data: s3://udacity-dend/log_data is loaded in to STAGING_EVENTS_TABLE

Points to to consider when loading into staging tables (STAGING_EVENTS_TABLE and STAGING_SONGS_TABLE)  
- Use the same column names as those in the data files, so that we can use "json auto" to load the data easily

This raw data is transformed and loaded in to dimensional tables (SONGPLAY_TABLE, USER_TABLE, SONG_TABLE, ARTIST_TABLE, TIME_TABLE). 
Points to consider while loading data into dimensional tables:

- Use distinct to avoid inserting duplicate rows (used fo USER_TABLE, SONG_TABLE and ARTIST_TABLE).
- load only page='NextSong'.
- Use 'IDENTITY' to autogenerate column values (used while creating SONGPLAY_TABLE).





## Example queries and results for song play analysis - provided in the below word document 

[Query Analysis.docx](https://github.com/avsprakash/Udacity/blob/9f8629f975731f969060f32dc7f25b6791901185/Data%20Engineering%20Nanodegree/Project3/Query%20Analysis.docx)


### Running the python scripts

- Run create_tables.py to create the staging and dimensional tables
- Run etl.py to load the staging and dimensional tables

### Files in the GIT repository

- create_tables.py - python code to create staging and dimensional tables
- dwh.cfg - configuration file to connect to Redshift database on aws
- etl.py - pythin code for etl pipeline that loads the staging and dimensional tables
- Query Analysis.docx - Some queries that have been run for analysis after loading the data and screenshots
- README.md - This readme file 
- songs_by_year.csv - Query results that group the songs by year
- sql_queries.py - SQL queries for deleting, creating and loading staging and dimensional tables