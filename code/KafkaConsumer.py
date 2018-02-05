import KafkaConstants
from csv import reader as csvReader
from io import BytesIO
from kafka import KafkaConsumer
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from datetime import datetime as date
from ast import literal_eval
from pickle import loads as pklLoad
from py4j.protocol import Py4JJavaError

################
### INIT GLOBALS    
################

conf = SparkConf().setAppName("InsiderMoviesConsumer")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
movieCountRec = 0 # for debugging
imCount = 0 # for debugging
movies = [] # holder to chunk movies

################
### DEFINE FUNCS    
################

def saveMovies():
    global movies
    global imCount

    rdds = sc.parallelize(movies) # list to rdd
    movies = [] # reset holder
    
    # cast values and remove useless columns
    rdds = rdds.map(lambda rdd: [float(rdd[2]), # budget
                                 map(lambda dic: dic['name'], literal_eval(rdd[3])), # genre
                                 rdd[20]] # title
                                 # release_date
                                 + (lambda dt: [dt.year, dt.month, dt.day])(date.strptime(rdd[14], '%Y-%m-%d')) 
                                 # revenue, vote_average, vote_count
                                 + [float(rdd[15]), float(rdd[22]), float(rdd[23])]) 

    # add movies inside insider cluster to external storage
    try:
        label = int(pklLoad(sc.pickleFile(KafkaConstants.LABEL_PATH).collect()[0])) # load cluster label
        model = pklLoad(sc.pickleFile(KafkaConstants.MODEL_PATH).collect()[0]) # load cluster model
                    
        # get insider movies
        im = rdds.filter(lambda rdd: rdd[0] >= KafkaConstants.MIN_BUDGET and
                                     rdd[6] >= KafkaConstants.MIN_REVENUE and
                                     rdd[7] >= KafkaConstants.MIN_VOTE_AVERAGE and
                                     rdd[8] >= KafkaConstants.MIN_VOTES) \
                 .filter(lambda rdd: model.predict([[rdd[0], rdd[6], rdd[7]]])[0] == label)
        
        num = im.count()
        if(num > 0):
            imCount += num # add insider movies to counter

            # convert data for mysql database
            im = im.map(lambda row: [rdd[0], str(rdd[1]), rdd[2], date(year=rdd[3], month=rdd[4], day=rdd[5]),
                                     rdd[6], rdd[7], rdd[8]])
            df = sqlContext.createDataFrame(im, KafkaConstants.DB_SCHEMA) # create dataframe with schema of the database

            # export movies to external storage (mysql database)
            prop = { "user": KafkaConstants.DB_USER,
                    "password": KafkaConstants.DB_PW,
                    "driver": "com.mysql.jdbc.Driver" }
            df.write.jdbc(url=KafkaConstants.DB_URL, 
                          table=KafkaConstants.DB_TABLE, 
                          mode="ignore",
                          properties=prop)
    except Py4JJavaError as e:
        print "Model not found"
    
    # save in hdfs with avro
    df = sqlContext.createDataFrame(rdds, schema=KafkaConstants.AVRO_SCHEMA)
    df.write.format(KafkaConstants.DATA_FORMAT) \
            .mode("append") \
            .partitionBy(KafkaConstants.PARTITION) \
            .save(KafkaConstants.HDFS_DESTINATION)
    
################
### START SCRIPT    
################

print ("Topic: " + KafkaConstants.TOPIC)

# connect to kafka broker
consumer = KafkaConsumer(KafkaConstants.TOPIC, 
                         bootstrap_servers=(KafkaConstants.BROKER_IP + ":9092"), 
                         group_id=KafkaConstants.GROUP_ID,
                         auto_offset_reset="earliest",
                         consumer_timeout_ms=KafkaConstants.CONSUMER_TIMEOUT) 

# stream movie rows
for msg in consumer: 
    holder = BytesIO(msg.value) # fake file
    rows = list(csvReader(holder)) # csv to list
    movieCountRec += len(rows) # add received movies to counter
    
    curDate = rows[0][14] # get date of current movies
    # cannot convert dates before 1990 into dataframe (year out of range)
    if(curDate[0:2] == "18"):  continue
    
    movies += rows
    if(len(movies) < KafkaConstants.CHUNK_SIZE): continue
    saveMovies()
    
# save last incomplete chunk
if(len(movies) > 0):
    saveMovies()
    
# finish
consumer.close()
print ("Movies recevied: " + str(movieCountRec))
print ("Insider Movies recevied: " + str(imCount))