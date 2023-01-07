from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Transforming JSON data").getOrCreate()

# Read the JSON file and create a dataframe
df = spark.read.option("multiLine", "true").json("./work/data.json")

# Transform a field in the dataframe
df = df.withColumn("age", df["age"]*2)

# Save the dataframe as an array of JSON objects in a JSON file
df.write.json("./work/output.json", mode="overwrite")

# Stop the SparkSession
spark.stop()