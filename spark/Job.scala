import org.apache.spark.sql.SparkSession

object Job {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Spark Job")
            .getOrCreate()
        
        val data = spark.read.option("multiline", "true").json("./work/data.json")
        
        data.printSchema()
        spark.stop()
    }
}