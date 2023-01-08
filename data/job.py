from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read the JSON file and create a dataframe
df = spark.read.option("multiLine", "true").json('file:///data/data.json')

# Transform a field in the dataframe
df = df.withColumn("age", df["age"]*2)

df.printSchema()
df.show()

# Save the dataframe as an array of JSON objects in a JSON file
single_df = df.coalesce(1)
single_df.write.json("file:///data/output", mode="overwrite")

# Stop the SparkSession
spark.stop()