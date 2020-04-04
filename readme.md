[Objective](Objective)\
[Data Model](Data-Model)\
[Tools and Technologies](Tools-and-Technologies)\
[Scaling considerations](Scaling-considerations)\
[Data Dictionary](Data-Dictionary)\
[Running the ETL Job](Running-the-ETL-Job)\
[Spark 2.4.3 Cluster Using Docker](Spark-243-Cluster-Using-Docker)\
[Resources](Resources)
[Copyright](Copyright)

# Objective

This repository contains code for the capstone ETL project as part of the Udacity Data Engineering Nanodegree program. For more details about the program, please see https://www.udacity.com/course/data-engineer-nanodegree--nd027.

The goal of the capstone project is to design a data pipeline to process a raw files into a set of fact/dimension tables that can then be consumed by a downstream application. In this particular case, I envision the downstream application to be a website that displays movie trends by year for different segments (genres, artists etc.).

The source datasets for this project come from IMDb (https://www.imdb.com/interfaces/). The dataset files can be accessed and downloaded from https://datasets.imdbws.com/. The data is refreshed daily at the source. For more information about what each dataset contains see [Data Dictionary](Data-Dictionary).

# Data Model

The data pipeline incorporates three files - `title.ratings.tsv.gz`, `name.basics.tsv.gz` and
`title.basics.tsv.gz` - and processes them into the following tables:

- **artists**: Contains one record for each artist
  - artistId: alphanumeric unique identifier of the name/person
  - primaryName: name by which the person is most often credited
  - birthYear: artist year of birth
  - deathYear: artist year of death. NULL if not applicable.
- **artists_knwnfor_titles**: Maps artists to their most known titles
  - artistId: alphanumeric unique identifier of the name/person
  - knownForTitles: alphanumeric unique identifier of the title
- **artists_prmry_profession**: Maps artists to their primary profession
  - artistId: alphanumeric unique identifier of the name/person
  - primaryProfession: profession (example: soundtrack, actor, actress, director etc.)
- **titles**: Contains one record for each title
  - titleId: alphanumeric unique identifier of the title
  - titleType: the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
  - primaryTitle: the more popular title / the title used by the filmmakers on promotional materials at the point of release
  - originalTitle: - original title, in the original language
  - isAdult: 0: non-adult title; 1: adult title
  - startYear: represents the release year of a title. In the case of TV Series, it is the series start year
  - endYear: TV Series end year. ‘\N’ for all other title types
  - runtimeMinutes: primary runtime of the title, in minutes
- **titles_genres**: Maps titles to genres
  - titleId: alphanumeric unique identifier of the title
  - genres: includes up to three genres associated with the title (one record per mapping)
- **titles_ratings**: Maps titles to their ratings
  - titleId: alphanumeric unique identifier of the title
  - averageRating: weighted average of all the individual user ratings
  - numVotes: number of votes the title has received
- **titles_genres_ratings**: Table containing ratings by genre and year
  - startYear: represents the release year of a title.
  - genres: genre of title
  - averageRating: mean of all ratings by genre-year
  - numVotes: total number of votes by genre-year
  - numTitles: total number of titles by genre-year

# Tools and Technologies

**Python and Apache Spark 2.4.3**\
The data processing is done using Python (PySpark) and Apache Spark. In our case, the files needed to be read from a url, processed in memory and written back to disk as parquet files. Python as a language was most suitable for this. Given the size of the data, Pandas wasn't suitable for processing the downloaded files. Spark dataframes offer great capabilities in terms of reading and writing files in a variety of data formats making it an ideal choice as a data processing engine. Spark dataframes also allow us to merge SQL-like syntax with Python code making it easier to write more complicated data processing code.

**Docker**\
The choice of docker to spin up a Spark cluster was influenced by two reasons - economic considerations and learning opportunity. Spinning up a Spark cluster in AWS-EMR or Azure might have been easier but it is also a more expensive choice. Spinning up a spark cluster locally helped me learn more about Spark installations and it's dependencies (such as Java version, Python version etc.)

**Delta Lake and Parquet**\
Rather than store the processed data as tables in a relational database or cloud datawarehouse such as Amazon Redshift, I decided to store them as Parquet files. This makes it more flexible for any downstream application(s) to consume this data. Parquet files are also more efficient in terms of space and for future I/O.

One downside of using files instead of tables is that files don't offer ACID properties available in databases. The Delta Lake project attempts to solve that by bringing database like features to parquet files (see https://docs.delta.io/latest/delta-intro.html for more information). Hence the processed files are stored as Delta Tables intead of raw parquet files. As of September 2019, Delta Lake is open source and work is underway to introduce upsert (update+insert) capabilities to parquet files. This functionality is already available in the Databricks version of Delta Lake but that's not open source.

# Scaling considerations

## If the data was increased by 100x.

The choice Apache Spark as a data processing engine makes scaling up for increased data sizes easy. If data was increased by 100x, I would do the following:

- Move the Spark cluster from a local docker implementation to Amazon EMR
- Increase number of workers in the cluster to handle additional workload

## If the pipelines were run on a daily basis by 7am.

This would require the implementation of a scheduler such as Apache Airflow that I haven't explored in this project. The pipelines can also be run locally by scheduling a cron-job but a scheduling tool such as Airflow offers greater flexibility.

## If the database needed to be accessed by 100+ people.

This problem can be handled by switching from storing the processed data as files to database tables in a cloud data warehouse such as Amazon Redshift, which has the capability to scale automatically based on user demand.

# Data Dictionary

Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set. The first line in each file contains headers that describe what is in each column. A ‘\N’ is used to denote that a particular field is missing or null for that title/name. The datasets used in this project are as follows:

### title.ratings.tsv.gz

Contains the IMDb rating and votes information for titles

- tconst (string) - alphanumeric unique identifier of the title
- averageRating – weighted average of all the individual user ratings
- numVotes - number of votes the title has received

**Number of Records: 1MM (as of 2019-09-29)**

### name.basics.tsv.gz

Contains the following information for names:

- nconst (string) - alphanumeric unique identifier of the name/person
- primaryName (string)– name by which the person is most often credited
- birthYear – in YYYY format
- deathYear – in YYYY format if applicable, else '\N'
- primaryProfession (array of strings)– the top-3 professions of the person
- knownForTitles (array of tconsts) – titles the person is known for

**Number of Records: 9.6MM (as of 2019-09-29)**

### title.basics.tsv.gz

Contains the following information for titles:

- tconst (string) - alphanumeric unique identifier of the title
- titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
- primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
- originalTitle (string) - original title, in the original language
- isAdult (boolean) - 0: non-adult title; 1: adult title
- startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
- endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
- runtimeMinutes – primary runtime of the title, in minutes
- genres (string array) – includes up to three genres associated with the title

**Number of Records: 6.2MM (as of 2019-09-29)**

---

# Running the ETL Job

The goal of the ETL job is to download a few datasets from IMDb, clean and manipulate them to create individual dimension and fact tables as Delta Lake tables (parquet files). The steps to run the ETL job locally are as follows -

- Spin up the Spark cluster in Docker using docker-compose command. For full instructions on creating Docker image and setting up Spark, see [Spark 2.4.3 Cluster Using Docker](#Spark-243-Cluster-Using-Docker)
  - `cd ./Spark`
  - `docker-compose up --scale spark-worker=4` sets up a cluster with 4 workers and 1 master.
- Run `./bin/python ./src/download_files.py` to download zip files from IMDb to `./data/tmp`.
- Run `./bin/python ./src/etl.py` to process the IMDb files and generated dimesion and fact tables as Delta Lake parquet files to `./data/delta/`. For more information on Delta Lake, see https://docs.delta.io/latest/delta-intro.html. The following delta tables are created -
  - artists
  - artists_knwnfor_titles
  - artists_prmry_profession
  - titles
  - titles_genres
  - titles_ratings
  - titles_genres_ratings
- Decomission Spark cluster using the docker-compose down command.
  - `cd ./Spark`
  - `docker-compose down`

# Spark 2.4.3 Cluster Using Docker

- Relevant files to spin up stand-alone Spark cluster using Docker are in `./Spark/`
  - docker-compose.yml
  - Dockerfile
  - start-master.sh
  - start-worker.sh
- Build the image from the `Dockerfile` using the docker build command. The image created is named aiyer/spark:latest but you can modify this by changing the `Dockerfile` and `docker-compose.yml`.
  - `cd ./Spark`
  - `docker build -t aiyer/spark:latest .`
- Use docker-compose to spin up a Spark cluster. `docker-compose.yml` provides the specifications for master/worker nodes.
  - `docker-compose up --scale spark-worker=4` sets up a cluster with 4 workers and 1 master.
- All docker containers are within a user-defined network called `spark-network`
  - `docker network create spark-network` can be used to create the network but this isn't necessary since it will be created automatically when you run docker compose.
  - Note that networks created automatically from the docker compose file follow the `image_network-name` naming convention. So if you use docker-compose to create the network, the network will be named `spark_spark-network` but that shouldn't matter since all we want is for all containers to be in the same network.
- Spark Jobs are submitted to the cluster using personal Mac as driver. Make sure that pyspark version matches the spark installation on containers, i.e. Spark 2.4.3
  - To install specific version of pyspark use `pip install pyspark==2.4.3`
  - OpenJDK 8 has been installed on driver machine (Mac) to support this. See https://adoptopenjdk.net/installation.html#x64_mac-jdk for installation instructions. This matches the JDK version on the Spark cluster (Spark only works on JDK8 as of 20-Aug-2019)
  - Python version on driver and cluster (master + workers) must match; Python 3.7.x in this case.
  - Connecting to AWS needs the JAR files [aws-java-sdk-1.7.4.jar](http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/) and [hadoop-aws-2.7.3.jar](http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar). This is copied to the `spark/jars` folder on the cluster machines

---

# Resources

- [A guide for setting up Apache Spark using Docker](https://towardsdatascience.com/a-journey-into-big-data-with-apache-spark-part-1-5dfcc2bccdd2)
- [Excellent 2 hour tutorial on Docker](https://youtu.be/fqMOX6JJhGo)
- [To configure spark to connect to AWS/S3](https://markobigdata.com/category/spark-configuration/)
- [Useful commands to clean up unused Docker resources and reclaim space](https://linuxize.com/post/how-to-remove-docker-images-containers-volumes-and-networks/)
- [Installing Spark Progress Bar in Jupyter](https://github.com/krishnan-r/sparkmonitor)

# Copyright
This project was submitted by Akshay Iyer as part of the Data Engineering Nanodegree At Udacity.

As part of Udacity Honor code, your submissions must be your own work, hence submitting this project as yours will cause you to break the Udacity Honor Code and the suspension of your account.

Me, the author of the project, allow you to check the code as a reference, but if you submit it, it's your own responsibility if you get expelled.

Copyright (c) 2019 Akshay Iyer
