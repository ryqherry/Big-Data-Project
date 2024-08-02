import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("genre_id", IntegerType, nullable = true),StructField("#tracks", IntegerType, nullable = true),StructField("parent", IntegerType, nullable = true),StructField("title", StringType, nullable = true),StructField("top_level", IntegerType, nullable = true)))
val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "false").schema(schema).load("Project/Data/genres.csv")
val output = df.withColumn("top_or_not", when(col("parent") === 0, 1).otherwise(0))
output.write.format("csv").option("header", "true").mode("overwrite").save("Project/Data/genres_clean")