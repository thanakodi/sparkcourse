from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge1 = rdd.mapValues(lambda x: (x, 1))
results1 = totalsByAge1.collect()
totalsByAge2 = totalsByAge1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
result2 = totalsByAge2.collect()
averagesByAge = totalsByAge2.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
