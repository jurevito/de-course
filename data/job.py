from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Create Spark session.
spark = SparkSession.builder.getOrCreate()

# Read the JSON file and create a dataframe
df = spark.read.json('file:///data/ingestion/chunk_0.json')

df = df.filter(F.length(F.col('doi')) > 0)

# Transform existing fields.
df = df.withColumn('title', F.trim(F.regexp_replace(F.col('title'), '\s+', ' ')))
df = df.withColumn(
    'update_date', 
    F.date_format(
        F.to_date(F.col('update_date'), 'yyyy-MM-dd'),
        'yyyy-MM-dd'
    )
)
df = df.withColumn('categories', F.split(F.col('categories'), ' '))

# Extract number of pages from "comments" field.
n_pages = F.trim(F.regexp_extract(F.col('comments'), '(\\d+)\\s*(?=pages)', 0))
df = df.withColumn(
    'n_pages',
    F.when(F.length(n_pages) == 0, None).otherwise(n_pages.cast(IntegerType()))
)

# Extract number of figures from "comments" field.
n_figures = F.trim(F.regexp_extract(F.col('comments'), '(\\d+)\\s*(?=figures)', 0))
df = df.withColumn(
    'n_figures',
    F.when(F.length(n_figures) == 0, None).otherwise(n_figures.cast(IntegerType()))
)

# Remove unnecessary fields.
drop_cols = ['abstract', 'license', 'authors', 'versions', 'comments']
df = df.drop(*drop_cols)

# Save the dataframe as an array of JSON objects in a JSON file.
single_df = df.coalesce(1)
single_df.write.json('file:///data/output', mode='overwrite')

# Stop the Spark Session.
spark.stop()