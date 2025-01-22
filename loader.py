import logging
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def configure_spark():
    """
    Configures the SparkConf object with the necessary settings.
    
    Returns:
        SparkConf: Configured SparkConf object.
    """
    try:
        logger.info("Configuring SparkConf...")
        conf = SparkConf()
        conf.set("spark.jars", "postgresql-42.7.5.jar")
        conf.set("spark.executor.memory", "3g")
        conf.set("spark.executor.cores", "1")
        conf.set("spark.cores.max", "1")
        logger.info("SparkConf configured successfully.")
        return conf
    except Exception as e:
        logger.error("Error while configuring SparkConf: %s", e, exc_info=True)
        raise


def create_spark_session(app_name="DataIngestion with PySpark", master="local[1]"):
    """
    Creates and returns a SparkSession object.
    
    Args:
        app_name (str): The name of the Spark application. Default is "DataIngestion with PySpark".
        master (str): The master URL to connect to. Default is "local[1]".
    
    Returns:
        SparkSession: Configured SparkSession object.
    """
    try:
        logger.info("Creating Spark session...")
        conf = configure_spark()
        spark = (
            SparkSession.builder.master(master)
            .appName(app_name)
            .config(conf=conf)
            .enableHiveSupport()
            .getOrCreate()
        )
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error("Error while creating Spark session: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    try:
        logger.info("Initializing the Spark application...")
        spark = create_spark_session()
        logger.info("Spark application initialized successfully.")
    except Exception as main_exception:
        logger.critical(
            "Critical failure in Spark application initialization: %s",
            main_exception,
            exc_info=True,
        )
        exit(1)  # Exit the program with a non-zero status
