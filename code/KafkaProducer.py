import KafkaConstants
from time import sleep
from csv import reader as csvReader, writer as csvWriter
from io import BytesIO
from itertools import groupby
from collections import deque
from kafka import KafkaProducer
from datetime import datetime as date, timedelta

print ("Topic: " + KafkaConstants.TOPIC)

creditRows = list(csvReader(open("credits.csv"))) # get credit csv rows as list
creditRows.pop(0) # remove column names row
credits = {credit[2]: credit[:2] for credit in creditRows} # convert to dictionary with id as key

movies = list(csvReader(open("movies_metadata.csv"))) # get movie csv rows as list
cols = len(movies[0]) # get column count
movies.pop(0) # remove column names row
print ("Movies: " + str(len(movies)))

# remove invalid rows (not enough values, no date)
movies = filter(lambda ele: len(ele) == cols and ele[14], movies)
# movies = filter(lambda ele: ele[14] == "1898-01-01" or ele[14] == "1874-12-09", movies) # multiple movies at the same day + only one movie (for debugging)
movies = sorted(movies, key = lambda line: line[14]) # sort by date
print ("Movies: " + str(len(movies)) + " (after filter)")

producer = KafkaProducer(bootstrap_servers=(KafkaConstants.BROKER_IP + ":9092")) # connect to kafka broker
curDate = date.strptime(movies[0][14], '%Y-%m-%d') # start day
getDate = True # flag for loop
rowDate = date(year = 1900, month = 1, day = 1) # declare holder

movieCount = 0 # count sent movies
for key, group in groupby(movies, key = lambda line: line[14]): # group and iterate by date
    holder = BytesIO() # fake file
    csvw = csvWriter(holder)
    getDate = True # always get the current date of the first item in group

    checker = movieCount
    for item in group: # iterate over movies in group
        if(getDate):
            rowDate = date.strptime(item[14], '%Y-%m-%d') # date of the movie row
            getDate = False

        if(item[5] in credits):
            movieCount += 1
            csvw.writerow(item + credits[item[5]]) # write movie with credit row
        else:
            print (str(rowDate) + " - Creditkey not found")
    if(checker == movieCount): continue # nothing wrote
    
    while(curDate <= rowDate):
        if(curDate == rowDate): 
            producer.send(KafkaConstants.TOPIC, holder.getvalue()) # send movie rows in csv format
            print ("send " + curDate.isoformat())
        # else: print ("skip " + curDate.isoformat())
        sleep(KafkaConstants.DAY_SLEEP) # wait for next day
        curDate = curDate + timedelta(days = 1) # next day
        
# finish
producer.close()
print ("Movies sent: " + str(movieCount))