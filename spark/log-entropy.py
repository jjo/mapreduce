#!/opt/spark/bin/spark-submit
import math
import re
from collections import Counter
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("local")
        .setAppName("My app")
        .set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)


def entropyCounter(cnt):
    total = float(sum(cnt.viewvalues()))
    prob = (val / total for val in cnt.viewvalues())
    entropy_val = - sum([p * math.log(p) / math.log(2.0) for p in prob])
    return entropy_val


def logline_extract(logline):
    match = re.match(r'^(\S+).*GET (\S+)', logline)
    if match:
        return match.groups()
    else:
        return ('None', 'None')

# files = 'hdfs:///user/ubuntu/ubuconla/star_wars_kid.log'
files = '/u/data/star_wars_kid.mini.log'
files = '/u/data/star_wars_kid.log'

lines = sc.textFile(files)
# smaller sample, to verif:
#   lines = sc.parallelize(sc.textFile(files).take(50))

# map each line to: (path, (ip,))
# ip as a Tuple, for easier Counter(ip) construct
path_ip = lines.map(
    lambda x: logline_extract(x)).map(
        lambda x: (x[1], x[0]))

path_concat_ips = path_ip.combineByKey(
    lambda v: Counter((v,)),
    lambda c, v: c.update((v,)) or c,
    lambda c1, c2: c1 + c2).cache()

path_entrop = path_concat_ips.map(
    lambda x: (x[0], round(entropyCounter(x[1]), 2)))

entrop_paths = path_entrop.map(
    lambda x: (x[1], x[0])).reduceByKey(lambda x, y: x + " " + y)

sorted_entrop_paths = entrop_paths.sortBy(lambda x: -x[0])

entrop_paths.saveAsTextFile('/tmp/mr-out.d')
