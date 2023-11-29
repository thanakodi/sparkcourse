from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")
    
#schemaPeople = people.cache()
people.createOrReplaceTempView("people")

print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

print("Running sample sql:")
spark.sql("SELECT * FROM people  WHERE age >= 13 AND age <= 19 ORDER BY age").show()

print("Make everyone 10 years older Part 2:")
spark.sql("SELECT name, age+10 FROM people  WHERE age >= 13 AND age <= 19 ORDER BY age").show()

spark.stop()

