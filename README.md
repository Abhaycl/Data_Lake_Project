# Data Lake Project Starter Code

The objective of this project is to apply what we've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3.

<!--more-->

[//]: # (Image References)

[image00]: ./images/relaciones.jpg "Database Schema for Sparkify"
[image01]: ./images/DataLake01.jpg "Bucket s3 destination"
[image02]: ./images/DataLake02.jpg "Bucket s3 destination"
[image03]: ./images/DataLake03.jpg "Reading song data"
[image04]: ./images/DataLake04.jpg "Song data extraction and transformation"
[image05]: ./images/DataLake05.jpg "Writing song table data to bucket S3"
[image06]: ./images/DataLake06.jpg "Artist data extraction and transformation"
[image07]: ./images/DataLake07.jpg "Writing artist table data to bucket S3"
[image08]: ./images/DataLake08.jpg "Reading log data"
[image09]: ./images/DataLake09.jpg "Users data extraction and transformation"
[image10]: ./images/DataLake10.jpg "Writing users table data to bucket S3"
[image11]: ./images/DataLake11.jpg "Creation of the timestamp and datetime column"
[image12]: ./images/DataLake12.jpg "Time data extraction and transformation"
[image13]: ./images/DataLake13.jpg "Writing time table data to bucket S3"
[image14]: ./images/DataLake14.jpg "Songplays data extraction and transformation"
[image15]: ./images/DataLake15.jpg "Writing time songplays data to bucket S3"
[image16]: ./images/DataLake16.jpg "Content bucked S3 destination"


---


#### How to run the program with your own code

For the execution of your own code, we head to the Project Workspace.

Running the etl.py script in a terminal to process the all the datasets. The script connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables.
```bash
  python etl.py
```


---

The summary of the files and folders within repo is provided in the table below:

| File/Folder              | Definition                                                                                                   |
| :----------------------- | :----------------------------------------------------------------------------------------------------------- |
| data/*                   | Folder that contains all the json files with the data used in this project.                                  |
| images/*                 | Folder containing the images of the project.                                                                 |
|                          |                                                                                                              |
| dl.cfg                   | Contains our AWS credentials.                                                                                |
| etl.py                   | Contains all the processing of reads data from S3, processes that data using Spark, and writes them back to S3. |
|                          |                                                                                                              |
| README.md                | Contains the project documentation.                                                                          |
| README.pdf               | Contains the project documentation in PDF format.                                                            |


---

**Steps to complete the project:**  

1. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS..


## [Rubric Points](https://review.udacity.com/#!/rubrics/2502/view)
### Here I will consider the rubric points individually and describe how I addressed each point in my implementation.  

---
## Scenario.

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Database Schema for Sparkify.

In our scenario, we have one dataset of songs where each file is in JSON format and contains metadata about a song and the artist of that song. The second dataset consists of log files in JSON format and simulates activity logs from a music streaming app (based on specified configurations).

Using the song and log datasets, the database schema looks like this:

![alt text][image00]


#### Star Schema.

A star schema is the simplest style of data mart schema. The star schema consists of one or more fact tables referencing to any number of dimension tables. It has some advantages like fast aggregation for analytics, simple queries for JOINs, etc.


## ETL Pipeline

Extract, transform, load (ETL) is the general procedure of copying data from one or more sources into a destination system which represents the data differently from, or in a different context than, the sources.


## Project Datasets

We'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: ```s3://udacity-dend/song_data```
- Log data: ```s3://udacity-dend/log_data```


#### Song Dataset

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to three files in this dataset.

```bash
    song_data/A/A/A/TRAAAAW128F429D538.json
    song_data/A/A/B/TRAABCL128F4286650.json
    song_data/A/A/C/TRAACCG128F92E8A55.json
```

And below is an example of what a single song file, TRAAAAW128F429D538.json, looks like.

```bash
{"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}
```

#### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset we'll be working with are partitioned by year and month. For example, here are filepaths to three files in this dataset.

```bash
    log_data/2018/11/2018-11-01-events.json
    log_data/2018/11/2018-11-02-events.json
    log_data/2018/11/2018-11-03-events.json
```

And below is an example of what the data in a log file, *2018-11-01-events.json*, looks like.

```bash
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```


#### Prerequisites

It is necessary that we have an AWS account. The following tasks must be realized within AWS:

1. In the IAM area you have to create a user with read and write permissions or full access to the S3 buckets area.
2. In the S3 area you have to create an S3 bucket, in our case we call it udacity-datalake-project-abhaycl.

![alt text][image01]
![alt text][image02]

*Possible help commands from the console:*
- Empty the content of the S3 bucket: ```aws s3 rm s3://udacity-datalake-project-abhaycl --recursive```
- Delete the previously created S3 bucket: ```aws s3 rb s3://udacity-datalake-project-abhaycl --force```

**Note:** If we have an AWS Free Tier account, each month only allows 2000 Put, Copy, Post or List Requests of Amazon S3, this may limit the reading of the song data files from the source S3 bucket (s3a://udacity-dend/song_data/) on our destination S3 bucket (s3://udacity-datalake-project-abhaycl).


#### Process song data (song_data directory).

We will perform ETL on the files in song_data directory to create two dimensional tables: songs table and artists table.

This is what a songs file looks like:

```bash
{"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}
```

We will proceed to read the data from the source S3 bucket, in the image below we can check the result of the task.

```bash
    df = spark.read.json(song_data)
```

![alt text][image03]


For the songs table, we will extract and transform the song data using only the columns corresponding to the songs table suggested in the project specifications, sorting the records by song ID and without duplicates.

```bash
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration") \
        .orderBy("song_id") \
        .drop_duplicates()
```

![alt text][image04]


We proceed to load our data into our destination S3 bucket to write the song table in parquet files partitioned by year and artist.

```bash
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"), mode="overwrite")
```

![alt text][image05]


For the artist table, we will extract and transform the song data using only the columns corresponding to the artist table suggested in the project specifications, sorting the records by artist ID and without duplicates.

```bash
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude") \
        .orderBy("artist_id") \
        .drop_duplicates()
```

![alt text][image06]


We proceed to load our data into our destination S3 bucket to write the artist table in parquet files.

```bash
    artists_table.write.parquet(os.path.join(output_data, "artists"), mode="overwrite")
```

![alt text][image07]


#### Process log data (log_data directory).

We will perform ETL on the files in log_data directory to create the remaining two dimensional tables: time and users, as well as the songplays fact table.

This is what a single log file looks like:

```bash
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
{"artist":null,"auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":0,"lastName":"Summers","length":null,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"GET","page":"Home","registration":1540344794796.0,"sessionId":139,"song":null,"status":200,"ts":1541106106796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
{"artist":"Des'ree","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":1,"lastName":"Summers","length":246.30812,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"You Gotta Be","status":200,"ts":1541106106796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
```

We will proceed to read the data from the source S3 bucket, in the image below we can check the result of the task.

```bash
    df = spark.read.json(log_data)
```

![alt text][image08]


First we filter by actions for song plays.

```bash
    df = df.where("page = 'NextSong'")
```

For the users table, we will extract and transform the log data using only the columns corresponding to the users table suggested in the project specifications, filtering the records that have value in the user id, sorting the records by user ID and without duplicates.

```bash
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level") \
        .filter("user_id <> ''") \
        .orderBy("user_id") \
        .drop_duplicates()
```

![alt text][image09]


We proceed to load our data into our destination S3 bucket to write the users table in parquet files.

```bash
    users_table.write.parquet(os.path.join(output_data, "users"), mode="overwrite")
```

![alt text][image10]


Creation of the timestamp and datetime column from original timestamp column.

```bash
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("timestamp", get_timestamp("ts"))

	get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime("%Y-%m-%d"))
    df = df.withColumn("datetime", get_datetime("ts"))
```

![alt text][image11]


For the time table, we will extract and transform the log data using only the columns corresponding to the time table suggested in the project specifications, sorting the records by start time and without duplicates.

```bash
    time_table = df.select(col("timestamp").alias("start_time"), hour("timestamp").alias("hour"), dayofmonth("timestamp").alias("day"), weekofyear("timestamp").alias("week"), month("timestamp").alias("month"), year("timestamp").alias("year"), date_format("timestamp", "u").alias("weekday")) \
        .orderBy("start_time") \
        .drop_duplicates()
```

![alt text][image12]


We proceed to load our data into our destination S3 bucket to write the song table in parquet files partitioned by year and month.

```bash
	time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"))
```

![alt text][image13]


We read, extract and transform the song data to use for songplays table.

```bash
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json")).select("song_id", "title", "artist_id", "artist_name", "year", "duration").drop_duplicates()
```

For the songplays table, we will extract and transform from joined song and log datasets using only the columns corresponding to the songplays fact table suggested in the project specifications, sorting the records by start time and user id.

```bash
    songplays_table = df.join(song_df, \
        (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration) & (year(df.timestamp) == song_df.year), "left_outer") \
        .select(df.timestamp.alias("start_time"), df.userId.alias("user_id"), df.level, song_df.song_id, song_df.artist_id, df.sessionId.alias("session_id"), df.location, df.userAgent.alias("user_agent"), year(df.timestamp).alias("year"), month(df.timestamp).alias("month")) \
        .orderBy("start_time", "user_id") \
        .withColumn("songplay_id", F.monotonically_increasing_id())
```

![alt text][image14]


We proceed to load our data into our destination S3 bucket to write the songplays table in parquet files partitioned by year and month.

```bash
	songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays"))
```

![alt text][image15]


In our target bucked S3 we will be able to check all saved data corresponding to the five defined tables.

![alt text][image16]


## Conclusion.

As the data engineer of the music streaming startup. We read our data from a source S3 bucked, with our spark cluster we extracted and transformed the data into five tables according to the project specifications and deposited them in our destination S3 bucked for further analysis or data processing.
