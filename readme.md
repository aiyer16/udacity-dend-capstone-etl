# Source Datasets
The source datasets for this project come from IMDb (https://www.imdb.com/interfaces/). The dataset files can be accessed and downloaded from https://datasets.imdbws.com/. The data is refreshed daily.

Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set. The first line in each file contains headers that describe what is in each column. A ‘\N’ is used to denote that a particular field is missing or null for that title/name. The datasets used in this project are as follows:

### title.ratings.tsv.gz
Contains the IMDb rating and votes information for titles
- tconst (string) - alphanumeric unique identifier of the title
- averageRating – weighted average of all the individual user ratings
- numVotes - number of votes the title has received

### name.basics.tsv.gz
Contains the following information for names:
- nconst (string) - alphanumeric unique identifier of the name/person
- primaryName (string)– name by which the person is most often credited
- birthYear – in YYYY format
- deathYear – in YYYY format if applicable, else '\N'
- primaryProfession (array of strings)– the top-3 professions of the person
- knownForTitles (array of tconsts) – titles the person is known for

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

# Spark 2.4.3 Cluster Using Docker

- Relevant files to spin up stand-alone Spark cluster using Docker are in `./Spark/`
    - docker-compose.yml
    - Dockerfile
    - start-master.sh
    - start-worker.sh
- Use docker-compose to spin up a Spark cluster. `docker-compose.yml` provides the specifications for master/worker nodes.
    - `docker-compose up --scale spark-worker=4` sets up a cluster with 4 workers and 1 master.
- All docker containers are within a user-defined network called `spark-network`
    - `docker network create spark-network` can be used to create the network but this isn't necessary since it will be created automatically when you run docker compose. 
- Spark Jobs are submitted to the cluster using personal Mac as driver. 
    - OpenJDK 8 has been installed on driver machine (Mac) to support this. See https://adoptopenjdk.net/installation.html#x64_mac-jdk for installation instructions. This matches the JDK version on the Spark cluster (Spark only works on JDK8 as of 20-Aug-2019)
    - Python version on driver and cluster (master + workers) must match; Python 3.7.x in this case.
    - Connecting to AWS needs the JAR files [aws-java-sdk-1.7.4.jar](http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/) and [hadoop-aws-2.7.3.jar](http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar). This is copied to the `spark/jars` folder on the cluster machines

# Resources
- [A guide for setting up Apache Spark using Docker](https://towardsdatascience.com/a-journey-into-big-data-with-apache-spark-part-1-5dfcc2bccdd2)
- [Excellent 2 hour tutorial on Docker](https://youtu.be/fqMOX6JJhGo)
- [To configure spark to connect to AWS/S3](https://markobigdata.com/category/spark-configuration/ )
- [Useful commands to clean up unused Docker resources and reclaim space](https://linuxize.com/post/how-to-remove-docker-images-containers-volumes-and-networks/)

---
