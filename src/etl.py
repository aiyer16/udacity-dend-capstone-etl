'''
Process IMDb datasets and store them as parquet files
'''
import datetime
import os
import pyspark.sql.functions as F

import util


def fix_year(col):
    '''
    Fix the year column by getting rid of bad values
    Params
        - col: column name
    '''
    # Get today's date
    now = datetime.datetime.now()

    fix_year_func = (
        F.when(F.col(col) < 1000, None).when(F.col(col) > now.year, None)
        .otherwise(F.col(col)))

    return fix_year_func


def process_artist_data(spark, source, dest):
    '''
    Process files related to artist data
    Params
        - spark: Active spark session object
        - source: Source directory/path for file
        - dest: Destination directory/path for file
    '''
    file = 'name.basics.tsv.gz'

    names_df_raw = spark.read.load(
        source + '/' + file,
        format="csv",
        sep="\t",
        inferSchema="true",
        header="true",
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        nullValue='\\N'
    )

    names_df = names_df_raw.withColumnRenamed("nconst", "artistId")

    names_df = names_df.withColumn("birthYear_fixed", fix_year('birthYear')) \
        .drop("birthYear") \
        .withColumnRenamed("birthYear_fixed", "birthYear")

    names_df = names_df.withColumn("deathYear_fixed", fix_year('deathYear')) \
        .drop("deathYear") \
        .withColumnRenamed("deathYear_fixed", "deathYear")

    # Prepare artist dataframes
    artists_df = names_df.select(
        "artistId", "primaryName", "birthYear", "deathYear")

    artists_prmry_prfsn_df = names_df.select("artistId",
                                             F.explode(
                                                 F.split(F.col("primaryProfession"), ","))
                                             .alias("primaryProfession"))

    artists_knwn_fr_ttls_df = names_df.select("artistId",
                                              F.explode(
                                                  F.split(F.col("knownForTitles"), ","))
                                              .alias("knownForTitles"))

    # Write artist dataframes to parquet
    artists_df.write.mode('overwrite').parquet(
        dest + "artists.parquet")

    artists_prmry_prfsn_df.write.mode('overwrite').parquet(
        dest + "artists_prmry_profession.parquet")

    artists_knwn_fr_ttls_df.write.mode('overwrite').parquet(
        dest + "artists_knwnfor_titles.parquet")


def process_title_data(spark, source, dest):
    '''
    Process files related to titles data
    Params
        - spark: Active spark session object
        - source: Source directory/path for file
        - dest: Destination directory/path for file
    '''
    file = 'title.basics.tsv.gz'

    title_basics_df_raw = spark.read.load(
        source + '/' + file,
        format="csv",
        sep="\t",
        inferSchema="true",
        header="true",
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        nullValue='\\N',
        # this will ignore using quotes as a qualifier. This helps reduce malformed records.
        quote=''
    )

    # Generate titles dataframes
    title_basics_df = title_basics_df_raw.withColumnRenamed(
        "tconst", "titleId")

    titles_df = title_basics_df.select("titleId", "titleType", "primaryTitle", "originalTitle",
                                       "isAdult", "startYear", "endYear", "runtimeMinutes")

    titles_genres_df = title_basics_df.select(
        "titleId", F.explode(F.split(F.col("genres"), ",")).alias("genres"))

    # Save processed dataframes to parquet
    titles_df.write.mode('overwrite').partitionBy(
        "startYear").parquet(dest + "titles.parquet")

    titles_genres_df.write.mode('overwrite').parquet(
        dest + "titles_genres.parquet")


def process_ratings_data(spark, source, dest):
    '''
    Process files related to ratings data
    Params
        - spark: Active spark session object
        - source: Source directory/path for file
        - dest: Destination directory/path for file
    '''
    file = 'title.ratings.tsv.gz'

    title_ratings_df_raw = spark.read.load(
        source + '/' + file,
        format="csv",
        sep="\t",
        inferSchema="true",
        header="true",
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        nullValue='\\N',
        # this will ignore using quotes as a qualifier. This helps reduce malformed records.
        quote=''
    )

    title_ratings_df = title_ratings_df_raw.withColumnRenamed(
        "tconst", "titleId")

    # Save processed dataframes to parquet
    title_ratings_df.write.mode('overwrite').parquet(
        dest + "title_ratings.parquet")


def main():
    '''
    Main routine
    '''
    # Set environment variables
    os.environ["JAVA_HOME"] = \
        "/Users/akshayiyer/Library/Java/JavaVirtualMachines/jdk8u222-b10/Contents/Home"

    spark = util.create_spark_session(
        "spark://127.0.0.1:7077", "s3.us-west-2.amazonaws.com")

    save_file_path = 'file:///Users/akshayiyer/Dev/GitHub/udacity-dend-capstone-etl/data/'
    temp_filepath = 'file:///Users/akshayiyer/Dev/GitHub/udacity-dend-capstone-etl/data/tmp'

    # Process all dimension files
    process_artist_data(spark, temp_filepath, save_file_path)
    process_title_data(spark, temp_filepath, save_file_path)
    process_ratings_data(spark, temp_filepath, save_file_path)


if __name__ == "__main__":
    main()
