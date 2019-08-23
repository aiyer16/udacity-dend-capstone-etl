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
