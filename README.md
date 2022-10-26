# Sparkify Data Warehouse

in this project, we'll apply data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to
>load data from S3 to staging tables on Redshift 
>execute SQL statements that create the analytics tables from these staging tables.

## Project Description

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app

## How to run the Python scripts

1. Create tables by running create_tables.py.
2. Execute ETL process by running etl.py.

## Project Template

1. create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
    
2. etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

3. sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.

4. README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

## Data Modeling

### fact Table
 
 songplays - records in event data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        
### Dimension Tables

users - users in the app
        user_id, first_name, last_name, gender, level
        
songs - songs in music database
        song_id, title, artist_id, year, duration
        
artists - artists in music database
       artist_id, name, location, lattitude, longitude
      
time - timestamps of records in songplays broken down into specific units
       start_time, hour, day, week, month, year, weekday


## Project Steps

#### Create Table Schemas


1. Design schemas for your fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in sql_queries.py
3. Complete the logic in create_tables.py to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to dwh.cfg.
7. Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

#### Build ETL Pipeline


1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete your redshift cluster when finished.





