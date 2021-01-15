# CSV IO Utils


def df_read_csv(job_context, path, header=False):
    """
    Reads a .csv file into a dataframe

    :param job_context: The preferred Context Manager
    :param path: The full path to .csv file
    :param header: Default=False; Does this .csv file have a header row?

    :return: Spark Dataframe
    """
    return job_context.spark.read.option("header", header).csv(path)

def df_write_csv(job_context, df, path):
    """
    Writes a dataframe to a .csv file

    :param job_context: The preferred Context Manager
    :param df: The dataframe to be written to .csv
    :param path: Full path where .csv will be written
    """
    bucket = job_context.bucket
    df.write.csv(f's3a://{bucket}/{path}',mode="overwrite")

