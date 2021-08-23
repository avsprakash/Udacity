import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN      = config.get("IAM_ROLE", "DWH_ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS_TABLE"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS_TABLE"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAY_TABLE"
user_table_drop = "DROP TABLE IF EXISTS USER_TABLE"
song_table_drop = "DROP TABLE IF EXISTS SONG_TABLE"
artist_table_drop = "DROP TABLE IF EXISTS ARTIST_TABLE"
time_table_drop = "DROP TABLE IF EXISTS TIME_TABLE"

# CREATE TABLES


#No primary key, transformations, load all raw data as it is
#use the same column names as those in the data files, so that we can use "json auto" to load the data easily

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS STAGING_EVENTS_TABLE 
                            (
                                artist varchar,
                                auth varchar, 
                                firstname varchar, 
                                gender varchar, 
                                iteminsession int, 
                                lastname varchar, 
                                length numeric, 
                                level varchar,  
                                location varchar,
                                method varchar,
                                page varchar,
                                registration varchar,
                                sessionid int,
                                song varchar,
                                status int,
                                ts numeric,
                                useragent varchar,
                                userid int
                             )

                        """)
                        
#No primary key, transformations, load all raw data as it is
#use the same column names as those in the data files, so that we can use "json auto" to load the data easily            

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS STAGING_SONGS_TABLE 
                            (
                                num_songs int NOT NULL,
                                artist_id varchar, 
                                artist_latitude numeric, 
                                artist_longitude numeric, 
                                artist_location varchar, 
                                artist_name varchar, 
                                song_id varchar, 
                                title varchar,  
                                duration numeric,
                                year int
                             )

                        """)
                        
#Use IDENTITY(0,1) to autogenerate songplay_id vaules while insertion

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS SONGPLAY_TABLE 
                            (
                                songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                start_time timestamp, 
                                user_id int NOT NULL, 
                                level varchar NOT NULL, 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id int, 
                                location varchar,  
                                user_agent varchar
                             )

                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS USER_TABLE 
                        (
                            user_id int PRIMARY KEY, 
                            first_name varchar NOT NULL, 
                            last_name varchar NOT NULL, 
                            gender varchar, 
                            level varchar NOT NULL
                         )
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS SONG_TABLE 
                        (
                            song_id varchar PRIMARY KEY, 
                            title varchar NOT NULL, 
                            artist_id varchar, 
                            year int NOT NULL, 
                            duration numeric NOT NULL
                        )
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS ARTIST_TABLE 
                           (   
                              artist_id varchar PRIMARY KEY, 
                              name varchar NOT NULL, 
                              location varchar, 
                              latitude numeric, 
                              longitude numeric
                           )
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS TIME_TABLE 
                        (
                            start_time timestamp PRIMARY KEY, 
                            hour int NOT NULL, 
                            day int NOT NULL, 
                            week int NOT NULL, 
                            month int NOT NULL, 
                            year int NOT NULL, 
                            weekday int NOT NULL
                        )
                    """)

# STAGING TABLES


staging_events_copy = ("""
    copy STAGING_EVENTS_TABLE from 's3://udacity-dend/log_data'
    iam_role '{}'
    json 'auto ignorecase' compupdate off region 'us-west-2';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
    copy STAGING_SONGS_TABLE from 's3://udacity-dend/song_data'
    iam_role '{}'
    json 'auto ignorecase' compupdate off region 'us-west-2';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO SONGPLAY_TABLE (start_time, user_id, level, song_id, artist_id, session_id, location,  user_agent) 
                             SELECT timestamp 'epoch' + STAGING_EVENTS_TABLE.ts/1000 * interval '1 second', STAGING_EVENTS_TABLE.userid, STAGING_EVENTS_TABLE.level,
                             STAGING_SONGS_TABLE.song_id, STAGING_SONGS_TABLE.artist_id, STAGING_EVENTS_TABLE.sessionid, STAGING_EVENTS_TABLE.location, STAGING_EVENTS_TABLE.useragent
                             FROM STAGING_SONGS_TABLE 
                             JOIN STAGING_EVENTS_TABLE 
                             on STAGING_SONGS_TABLE.artist_name = STAGING_EVENTS_TABLE.artist
                             and STAGING_SONGS_TABLE.title = STAGING_EVENTS_TABLE.song
                             WHERE page=\'NextSong\'

""")

user_table_insert = (""" INSERT INTO USER_TABLE (user_id, first_name, last_name, gender, level)
                        SELECT distinct(userid), firstname, lastname, gender, level FROM STAGING_EVENTS_TABLE
                                             WHERE page=\'NextSong\'
                            
""")

song_table_insert = (""" INSERT INTO SONG_TABLE(song_id, title, artist_id, year, duration)
                        SELECT distinct(song_id), title, artist_id, year, duration FROM STAGING_SONGS_TABLE
""")

artist_table_insert = ("""INSERT INTO ARTIST_TABLE(artist_id, name, location, latitude, longitude)
                            SELECT distinct(artist_id), artist_name, artist_location, artist_latitude, artist_longitude FROM STAGING_SONGS_TABLE
""")

#Extract the timestamp in to ts1, by adding the number of seconds (convert from millisecond from the field ts) to the start of Unix "epoch"  - 00:00:00 UTC on 1 January 1970
#and use this ts1, to get various values like hour, day, week, month, year and weekday

time_table_insert = ("""INSERT INTO TIME_TABLE (start_time, hour, day, week, month, year, weekday)
                        select timestamp 'epoch' + ts/1000 * interval '1 second' as ts1,to_number(to_char(ts1,'HH24'),'99'),
                        to_number(to_char(ts1,'DD'),'99'), to_number(to_char(ts1,'WW'),'99'),
                        to_number(to_char(ts1,'MM'),'99'),to_number(to_char(ts1,'YYYY'),'9999'),to_number(to_char(ts1,'D'),'9')
                        FROM STAGING_EVENTS_TABLE
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
