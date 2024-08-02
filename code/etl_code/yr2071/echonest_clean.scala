import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Project/Data/echonest.csv")
val output = df.select("track_id", "echonest audio_features acousticness", "echonest audio_features danceability", "echonest audio_features energy", "echonest audio_features instrumentalness", "echonest audio_features liveness", "echonest audio_features speechiness", "echonest audio_features tempo", "echonest audio_features valence")
val output_coalesced = output.coalesce(1)
output_coalesced.write.format("csv").option("header", "true").mode("overwrite").save("Project/Data/echonest_clean")