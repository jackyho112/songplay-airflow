# Data Modeling with Redshift and Airflow

In this project, we are extracting data regarding song plays from JSON files into an Amazon Redshift data warehouse with Airflow.

## Dataset 

The dataset comes from the [Million Song Dataset](http://millionsongdataset.com/). The data should contain two folders, one for the log and one for the songs. The song dataset should contain metadata about a song and its artist. The log dataset should contain acitivity records of played songs.

## Schema

For the database, we are creating the following tables.

songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
users - user_id, first_name, last_name, gender, level
songs - song_id, title, artist_id, year, duration
artists - artist_id, name, location, latitude, longitude
time - start_time, hour, day, week, month, year, weekday

## Creating a Redshift cluster

`create_redshift_cluster.ipynb` contains instructions on how to create a Redshift server programmatically.

## Running Airflow

In the Airflow folder,

to start the web sever:
```bash
airflow webserver -p 8080
```

to start the scheduler:
```bash
airflow scheduler
```

For more information on Airflow, go to its [site](http://airflow.apache.org/docs/stable/)


