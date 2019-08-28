'''
Download files from IMDb to Local directory.
Files are downloaded to ./data/tmp
'''
import os
import util


def main():
    '''
    Main routine
    '''
    base_url = 'https://datasets.imdbws.com'

    files_list = [
        "name.basics.tsv.gz",
        "title.akas.tsv.gz",
        "title.basics.tsv.gz",
        "title.crew.tsv.gz",
        "title.episode.tsv.gz",
        "title.principals.tsv.gz",
        "title.ratings.tsv.gz"]

    temp_filepath = os.path.abspath(
        os.path.join(os.path.abspath(''), os.pardir, 'data', 'tmp'))

    util.empty_local_directory(temp_filepath)

    util.download_files_to_local(base_url, files_list, temp_filepath)


if __name__ == "__main__":
    main()
