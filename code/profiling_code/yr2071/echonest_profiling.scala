import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Project/Data/echonest.csv")
val numRows = df.count()
val idArtistList = df.select("track_id", "echonest metadata artist_name")
val num_distinct_artists = idArtistList.select("echonest metadata artist_name").distinct().count()
val kurtVileList = idArtistList.filter($"echonest metadata artist_name" === "Kurt Vile")
val numKurtVileTracks = kurtVileList.count()