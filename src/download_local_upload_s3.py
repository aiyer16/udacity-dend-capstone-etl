'''
Download from http to local and upload to S3
'''
import os
import util


def main():
    '''
    Main routine
    '''
    # Set file/url variables
    temp_filepath = os.path.abspath(os.path.join(
        os.path.dirname(__file__), os.pardir, 'data', 'tmp'))
    base_url = 'https://datasets.imdbws.com/'

    files_list = [
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"
        ]

    # Main Section
    util.download_files_to_local(base_url, files_list, temp_filepath)

    util.upload_files_to_s3(
        "us-east-2",
        "aiyer-udacity-dend",
        files_list,
        temp_filepath
        )

    util.empty_local_directory(temp_filepath)


if __name__ == "__main__":
    main()
