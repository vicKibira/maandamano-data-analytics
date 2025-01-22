import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace
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


def create_spark_session(app_name="data ingestion with pySpark", master="local[1]"):
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


def read_data(spark, file_path):
    """
    This function reads the maandamano csv data using spark

    Args:
        spark(SparkSession): The actual sparksession
        file_path: Path to the csv file

    Returns:
        Dataframe: The loaded spark dataframe
    """
    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )
        logger.info("Data successfully loaded into a DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Failed to read data from {file_path}. Error: {e}")
        raise


def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleans and standardizes a Spark DataFrame.

    Args:
        df (DataFrame): The loaded DataFrame to clean.

    Returns:
        DataFrame: A cleaned DataFrame with standardized column names 
                   and transformed string columns.
    """
    try:
        # Standardize column names to lowercase
        df_cleaned = df.toDF(*[c.lower() for c in df.columns])

        def clean_string_columns(df: DataFrame) -> DataFrame:
            """
            Transforms string columns in the DataFrame:
            - Converts all values to lowercase.
            - Replaces spaces with underscores.

            Args:
                df (DataFrame): The DataFrame to transform.

            Returns:
                DataFrame: A DataFrame with cleaned string columns.
            """
            for col_name, dtype in df.dtypes:
                if dtype == 'string':
                    df = df.withColumn(
                        col_name,
                        lower(regexp_replace(col(col_name), ' ', '_'))
                    )
            return df

        # Clean string columns
        cleaned_df = clean_string_columns(df_cleaned)
        logger.info("Data cleaned successfully.")
        return cleaned_df

    except Exception as e:
        logger.error("Error while cleaning data: %s", e, exc_info=True)
        raise


def write_to_db(cleaned_df, db_url, db_table, user, password, driver="org.postgresql.Driver", batch_size=10000, mode="overwrite"):
    """
    Writes a cleaned DataFrame to the specified database table.

    Args:
        cleaned_df (DataFrame): The cleaned DataFrame to write.
        db_url (str): JDBC URL of the database.
        db_table (str): Target database table name.
        user (str): Database username.
        password (str): Database password.
        driver (str): JDBC driver class name. Default is "org.postgresql.Driver".
        batch_size (int): Number of rows per batch to write. Default is 10000.
        mode (str): Write mode ("overwrite", "append", etc.). Default is "overwrite".

    Returns:
        None
    """
    try:
        logger.info(f"Preparing to write DataFrame to table '{db_table}' in the database...")

        # Validate DataFrame
        if cleaned_df is None or cleaned_df.rdd.isEmpty():
            logger.warning("DataFrame is empty. Skipping write operation.")
            return

        # Write DataFrame to the database
        cleaned_df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", db_table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .option("batchsize", str(batch_size)) \
            .mode(mode) \
            .save()

        logger.info(f"Data successfully written to table '{db_table}' in the database.")
    except Exception as e:
        logger.error(f"Failed to write data to table '{db_table}'. Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        logger.info("Starting the data ingestion and processing pipeline...")

        # Get credentials from environment variables
        db_url = os.getenv("DB_URL")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_table = os.getenv("DB_TABLE")

        # Check if credentials are set in environment variables
        if not db_url or not db_user or not db_password or not db_table:
            logger.error("Database credentials are not set in the environment variables.")
            raise ValueError("Missing required environment variables for database credentials.")

        # Create Spark session
        spark = create_spark_session("Maandamano Data Analysis")

        # Path to your maandamano dataset
        data_file = "maandamano_kenya_dataset.csv"

        # Read data
        df_maandamano = read_data(spark, data_file)

        # Clean data
        df_cleaned = clean_data(df_maandamano)

        # Write data to the database
        write_to_db(df_cleaned, db_url, db_table, db_user, db_password)

        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.error("Pipeline failed. Error: %s", e, exc_info=True)
