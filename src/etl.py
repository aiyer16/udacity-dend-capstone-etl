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
    '''
    # Get today's date
    now = datetime.datetime.now()

    fix_year_func = (
        F.when(F.col(col) < 1000, None).when(F.col(col) > now.year, None)
        .otherwise(F.col(col)))

    return fix_year_func


def process_name_basics(spark):
    '''
    Process name.basics.tsv.gz
    '''
    temp_filepath = 'file:///Users/akshayiyer/Dev/GitHub/udacity-dend-capstone-etl/data/tmp'
    file = 'name.basics.tsv.gz'

    names_df_raw = spark.read.load(
        temp_filepath + '/' + file,
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
    save_file_path = 'file:///Users/akshayiyer/Dev/GitHub/udacity-dend-capstone-etl/data/'

    artists_df.write.mode('overwrite').parquet(
        save_file_path + "artists.parquet")

    artists_prmry_prfsn_df.write.mode('overwrite').parquet(
        save_file_path + "artists_prmry_profession.parquet")

    artists_knwn_fr_ttls_df.write.mode('overwrite').parquet(
        save_file_path + "artists_knwnfor_titles.parquet")


def main():
    '''
    Main routine
    '''
    # Set environment variables
    os.environ["JAVA_HOME"] = \
        "/Users/akshayiyer/Library/Java/JavaVirtualMachines/jdk8u222-b10/Contents/Home"

    spark = util.create_spark_session(
        "spark://127.0.0.1:7077", "s3.us-west-2.amazonaws.com")

    process_name_basics(spark)


if __name__ == "__main__":
    main()
