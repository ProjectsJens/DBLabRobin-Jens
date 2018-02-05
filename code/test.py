import KafkaConstants
from pyspark import SparkContext, SparkConf


from pickle import loads as pklLoad
from py4j.protocol import Py4JJavaError

conf = SparkConf().setAppName("myFirstApp")
sc = SparkContext(conf=conf)

try:
    label = pklLoad(sc.pickleFile(KafkaConstants.LABEL_PATH).collect()[0])
    model = pklLoad(sc.pickleFile(KafkaConstants.MODEL_PATH).collect()[0])

    print label
    print model.predict([[1,1,2]])
except Py4JJavaError as e:
    print "Model not found"


