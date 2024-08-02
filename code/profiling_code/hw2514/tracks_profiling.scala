import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Project/Data/tracks.csv")
val numRows = df.count()
val cols = df.columns
val numDistinctTracks = df.select("track_id").distinct().count()
// artist associated_labels not null numbers
val numNotNullArtistAssociatedLabels = df.filter(col("artist associated_labels").isNotNull).count()
