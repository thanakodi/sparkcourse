from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseline(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
#mycustomeroders = lines.map(parseline)
#customeroders = mycustomeroders.reduceByKey(lambda x, y: x + y).sortByKey(ascending=False)
# customeroders = lines.map(parseline) \
#    .reduceByKey(lambda x, y: x + y).sortByKey(ascending=False)
#results = customeroders.collect()

results = lines.map(parseline) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=True) \
    .collect()

for cust, amount in results:
    print(cust, "\t{:.2f}".format(amount))
