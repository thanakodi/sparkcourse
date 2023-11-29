from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

customSchema = StructType([
    StructField("ID", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("numFriends", IntegerType(), True)
    ])

people = spark.read.option("header", "true").schema(customSchema)\
    .csv("file:///SparkCourse/fakefriends.csv")

people.createOrReplaceTempView("people")
people.printSchema()
people.select("*").show()

#friendsByAge = spark.sql("SELECT age, numFriends FROM people GROUP BY AGE,numFriends ORDER BY age DESC")
#friendsByAge = spark.sql("SELECT age, SUM(numFriends)/count(age) AS friendsAverage from people GROUP BY age ORDER BY friendsAverage DESC")
#friendsByAge = spark.sql("SELECT age, AVG(numFriends) AS friendsAverage \
# from people GROUP BY age ORDER BY friendsAverage DESC")
#friendsByAge.show()
friendsByAge = people.select("age","numFriends")

sortedfriendsByAge = friendsByAge.groupBy("age").agg(func.round(func.avg("numFriends"), 2).alias("friends")).sort("age")
sortedfriendsByAge.show()

#averageFriendsByAge = spark.sql("SELECT age, SUM(numFriends)/count(age) AS friendsAverage from friends GROUP BY age ORDER BY friendsAverage DESC")
#averageFriendsByAge.show()
spark.stop()
