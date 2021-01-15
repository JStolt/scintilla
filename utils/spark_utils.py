# Spark and Logger Utils
from pyspark.sql import SparkSession

def get_logger(spark):
    """
    Creates a logger.
    
    :param spark: The spark session to be logged

    :return: Logger
    """
    sc = spark.sparkContext

    logger = sc._jvm.org.apache.log4j.LogManager.getLogger("Scintilla")
    logger.setLevel(sc._jvm.org.apache.log4j.Level.ALL)

    logger.info("Scintilla logger started...")

    return logger


def get_spark(app_name):
    """
    Establishes and returns a generic spark session

    :param app_name: The name of the Spark Application

    :return: Named Spark Session
    """
    spark = (SparkSession
             .builder
             .appName(app_name)
             .config("spark.jars.packages")
             .config("spark.sql.parquet.writeLegacyFormat", "true")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             # .enableHiveSupport()
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")

    return spark

