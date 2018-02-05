from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ShortType, FloatType, ArrayType, IntegerType, DateType

DAY_SLEEP = 0.001 # 1 ms
BROKER_IP = "10.0.2.15"
# BROKER_IP = "127.0.0.1"
GROUP_ID = None
# GROUP_ID = "InsiderMovies" # doesnt work (cannot receive messages)
TOPIC = "KafkaMovieStream"
CONSUMER_TIMEOUT = 60 * 1000 # 1min - set to float('inf') for endless consuming
CHUNK_SIZE = 250 # min movies before hdfs export (prevent single row files)

DATA_FORMAT = "com.databricks.spark.avro"
PARTITION = "year"
AVRO_SCHEMA = StructType([StructField("budget", DoubleType(), True), 
                          StructField("genres", ArrayType(StringType(), True), True),
                          StructField("title", StringType(), True), 
                          StructField("year", ShortType(), True), 
                          StructField("month", ShortType(), True), 
                          StructField("day", ShortType(), True),
                          StructField("revenue", DoubleType(), True),
                          StructField("vote_average", FloatType(), True),
                          StructField("vote_count", FloatType(), True)])

HDFS_DESTINATION = "/movies"
MIN_VOTES = 1
MIN_BUDGET = 999 # wrong values (f.e. 200 instead of 200.000.000)
MIN_REVENUE = 1 # zero => no data
MIN_VOTE_AVERAGE = 7

MODEL_PATH = "/insider_movies_model.pkl"
LABEL_PATH = "/insider_movies_label.pkl"

DB_URL ="jdbc:mysql://localhost/insider_movies"
DB_TABLE = "movie"
DB_USER = "root"
DB_PW = "cloudera"
DB_SCHEMA = StructType([StructField("budget", DoubleType(), True), 
                        StructField("genres", StringType(), True),
                        StructField("title", StringType(), True), 
                        StructField("date", DateType(), True),
                        StructField("revenue", DoubleType(), True),
                        StructField("vote_average", FloatType(), True),
                        StructField("vote_count", FloatType(), True)])