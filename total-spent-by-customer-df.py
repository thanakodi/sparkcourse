from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

schema = StructType([
    StructField("customerID", StringType(), True),
    StructField("itemID", StringType(), True),
    StructField("amount", FloatType(), True)])

df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()
df.show()

totalSpentByCustomer = df.groupBy("customerID").agg(func.round(func.sum("amount"), scale=2).alias("TotalAmount")) \
    .sort(func.col("TotalAmount").desc())

totalSpentByCustomer.show(totalSpentByCustomer.count())

spark.stop()
