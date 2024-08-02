import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Project/Data/features.csv")
val output = df.select(col("track_id") +: df.columns.filter(_.contains("mfcc")).map(col): _*)
val output_coalesced = output.coalesce(1)
output_coalesced.write.format("csv").option("header", "true").mode("overwrite").save("Project/Data/features_clean")