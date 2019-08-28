'''
Contains all utility functions
'''
import urllib.request
import os
import configparser
import zipfile
import boto3
from pyspark.sql import SparkSession


def download_files_to_local(base_url, files_list, local_directory):
    '''
    Download files from HTTP source to local destination
    Params:
        base_url - HTTP source prefix
        files_list - List of files in HTTP source such that
            full file path = base_url/file
        local_directory - Local directory to download files


    '''
    for file in files_list:
        file_source = base_url + "/" + file
        file_destination = os.path.join(local_directory, file)

        urllib.request.urlretrieve(file_source, filename=file_destination)


def upload_files_to_s3(
        region_name,
        bucket_name,
        files_list,
        local_directory,
):
    '''
    Upload files from a local directory to an S3 bucket
    Params:
        region_name - AWS Region Name (Example: us-east-1, us-west-2 etc.)
        bucket_name - S3 bucket name
        files_list - List of files to upload
        local_directory - Local directory containing files_list
    '''
    # Read credentials
    config = configparser.ConfigParser()
    config_filename = os.path.abspath(
        os.path.join(os.path.dirname(__file__),
                     os.pardir,
                     '.aws', 'access_keys.cfg')
    )
    config.read_file(open(config_filename))

    aws_key = config.get('AWS', 'KEY')
    aws_secret = config.get('AWS', 'SECRET')

    s3_resource = boto3.resource(
        's3',
        region_name=region_name,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
    )

    for file in files_list:
        file_source = os.path.join(local_directory, file)

        s3_resource.Bucket(bucket_name).upload_file(file_source, file)


def empty_local_directory(local_directory):
    '''
    Clears all files in local directory
    Params:
        local_directory - Directory to clear
    '''
    for file in os.listdir(local_directory):
        if os.path.isfile(os.path.join(local_directory, file)):
            os.unlink(os.path.join(local_directory, file))


def unzip_single_file(source, dest):
    '''
    Unzip a single zipped file to a destination directory
    Params:
        source - File path + name of source zip file
        dest - Destination directory for unzipped files
    '''
    with zipfile.ZipFile(source, 'r') as zip_obj:
        zip_obj.extractall(dest)


def create_spark_session(master, endpoint=None):
    '''
    Create Spark Session
    Params
        - master : Master spark node host IP
        - endpoint : AWS Region Endpoint
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("fs.s3a.endpoint", endpoint) \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .appName("udacity-dend-capstone-etl-proj") \
        .master(master) \
        .getOrCreate()

    return spark
