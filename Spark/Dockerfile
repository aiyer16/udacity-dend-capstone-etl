FROM ubuntu:latest

RUN apt-get update 
RUN apt-get --assume-yes install wget tar bash openjdk-8-jdk python3.7 python3-pip && \
    ln -s /usr/bin/python3.7 /usr/bin/python

#RUN echo "alias python=python3" >> ~/.bash_aliases

RUN wget http://apache.mirror.anlx.net/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz 
RUN wget http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
RUN wget http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar


RUN tar -xzf spark-2.4.3-bin-hadoop2.7.tgz
RUN mv spark-2.4.3-bin-hadoop2.7 /spark 
RUN mv aws-java-sdk-1.7.4.jar /spark/jars
RUN mv hadoop-aws-2.7.3.jar /spark/jars
RUN rm spark-2.4.3-bin-hadoop2.7.tgz

COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh

RUN mkdir '/opt/data'