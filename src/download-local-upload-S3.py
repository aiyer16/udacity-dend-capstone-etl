import urllib.request
import os
import configparser
import boto3

def download_files_to_local (base_url, files_list, local_directory):
    for file in files_list:
        file_source = base_url + "/" + file
        file_destination = os.path.join(local_directory,file)
            
        urllib.request.urlretrieve(file_source,filename=file_destination)

def upload_files_to_S3 (region_name, bucket_name, files_list, local_directory):
         
    s3 = boto3.resource(
        's3',
        region_name = region_name,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    for file in files_list:
        file_source = os.path.join(local_directory,file)

        s3.Bucket(bucket_name).upload_file(file_source, file)


def empty_local_directory (local_directory):
    for file in os.listdir(local_directory):
        if os.path.isfile(os.path.join(local_directory,file)):
            os.unlink(os.path.join(local_directory,file))

if __name__ == "__main__":
    # Read credentials
    config = configparser.ConfigParser()
    config_filename = os.path.abspath(\
            os.path.join(os.path.dirname( __file__ ), \
            os.pardir, \
            '.aws','access_keys.cfg') \
            )
    config.read_file(open(config_filename))

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    # Set file/url variables
    temp_filepath = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, 'data', 'tmp'))
    base_url = 'https://datasets.imdbws.com/'

    files_list = ["name.basics.tsv.gz"\
    ,"title.akas.tsv.gz"\
    ,"title.basics.tsv.gz"\
    ,"title.crew.tsv.gz"\
    ,"title.episode.tsv.gz"
    ,"title.principals.tsv.gz"\
    ,"title.ratings.tsv.gz" ]

    # Main Section        
    download_files_to_local (base_url, files_list, temp_filepath)
    
    upload_files_to_S3("us-east-2", "aiyer-udacity-dend", files_list, temp_filepath)
    
    empty_local_directory(temp_filepath)

    