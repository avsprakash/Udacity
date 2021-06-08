Project goal: Data Modeling using python and Postgres for a music streaming startup called Sparkify.
The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We need to create a database schema and build the ETL pipeline to help them on the analysis of user and song data.


Pythnon scripts and purpose
1. create_tables.py - has functions to create database, drop all tables and create the tables.
    The main function calls them one after the other.

2. etl.py - This script file has functions to process the songs data and logs data. And load this data in to the fact table - songplays and
    dimension tables - users, songs, artists and time
 
3. sql_queries.py - This file has all the sql query strings needed to drop the tables, create the tables, insert data in to the tables and select data
    from the songs and artists table.