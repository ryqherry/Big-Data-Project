import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

// Create SparkSession
val spark = SparkSession.builder().appName("TrackCleaner").getOrCreate()

// Read the input CSV file
val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("multiline", "true").load("Project/Data/tracks.csv") // Update with your bucket name

// Define the cleaning UDF
val cleanGenresUDF = udf { (genreList: String) =>
  if (genreList == null || genreList.isEmpty) {
    true
  } else {
    try {
      val genres = genreList.stripPrefix("[").stripSuffix("]").split(",").map(_.trim)
      genres.forall(genre => genre.isEmpty || genre.forall(_.isDigit))
    } catch {
      case _: Throwable => false
    }
  }
}

// Step 1: Filter out rows with null or non-numeric track_id
val cleanedDF = df.filter(col("track_id").isNotNull && col("track_id").cast(StringType).rlike("^\\d+$"))

// Step 2: Filter out rows with invalid track genres_all
val cleanedDFWithGenres = cleanedDF.filter(cleanGenresUDF(col("track genres_all")))

// Step 3: Add a new column to indicate presence of track genre_top
val updatedDf = cleanedDFWithGenres.withColumn("has_top_genre", when(col("track genre_top").isNotNull, 1).otherwise(0))

// step 4: select only needed columns
// track_id, artist favorites, artist id, set split, set subset, track favorites, track genre_top, track genres, track genres_all, track interest, track listens, track title
val selectedCols = Seq("track_id", "artist favorites", "artist id", "set split", "set subset", "track favorites", "track genre_top", "track genres", "track genres_all", "track interest", "track listens", "track title", "has_top_genre")
val selectedDf = updatedDf.select(selectedCols.map(col): _*)

// Write the cleaned DataFrame to CSV
selectedDf.write.format("csv").option("header", "true").mode("overwrite").save("Project/Data/tracks_clean") // Update with your bucket name


