// analysis tracks
// calculate potential correaltion between genre and listen times
// columns to notice: 
// album favorites, album listens
// artists favorites, artist tags, 
// track duration, track favorites, track genre_top, track genres, track genres_all, track interest, track listens,
// track date_created, track language_code

// Motivation: in order to predict the genre of some music, we consider the following possible related aspects
// 1. artist preference: different artist could have a different habit for different genres of music, with some genre focused,
//      know the artist's id helps predict the genre
// 2. geography issues: artist living in different places could tend to create music of different genre affected by the local environemnt
//      know the artist's geographical info helps decide the music's genre
// 3. release time: some songs of the same genre may tend to appear in close time periods
//      know the track's release time may help to predict the genre of the music
// 4. artist age: artist at different age could have a preference for different genres of music
// 5. language: different language may have a nature favor for different genres


// 1. analyze the distribution of genres of all the music by the same artist
// figure out the most 2 probable genres for the artist
// columns used: artist id, track genres_all
// steps: first, explode the track genres_all column into separate genre id rows
//      secondly, group by artist id and genre id, and count the number of each pair
//      thirdly, calculated the likelihood of a music be of the genre of the artist id
//      finally, group by the artist id and genre id, and order by the pair of the highest likelihood
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("multiline", "true").load("Project/Data/tracks.csv")
val explodedDF = df.withColumn("genre id", explode(split(col("track genres_all"), ",")))
// Group by artist_id and genre_id, then count the occurrences
val artistGenreCounts = explodedDF.groupBy("artist id", "genre id").count().orderBy(desc("count"))
// Calculate the total count of genre pairs for each artist_id
val artistTotalCounts = artistGenreCounts.groupBy("artist id").agg(sum("count").alias("total_count"))
// Join the genre counts with the total counts to calculate frequencies
val artistGenreFreq = artistGenreCounts.join(artistTotalCounts, Seq("artist id"), "inner").withColumn("frequency", col("count") / col("total_count")).orderBy(desc("count"))
// Show the result, including frequency of each pair relative to total count
artistGenreFreq.show()
// only show the top two most probable genre for each artist id
// Define a window specification to rank genres within each artist_id partition
import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("artist id").orderBy(desc("frequency"))
// Add a rank column based on genre frequency within each artist_id group
val rankedGenres = artistGenreFreq.withColumn("rank", rank().over(windowSpec))
// Filter to keep only the top two genres (rank 1 and 2) for each artist_id
val topTwoGenresPerArtist = rankedGenres.filter(col("rank") <= 2).drop("rank") // Drop the rank column after filtering
// Show the top two genres per artist_id
topTwoGenresPerArtist.orderBy(desc("total_count")).show()   // ordered from highest total_count to lowest for each artist with only first two probable genre



// 2. analyze the correlation between geographical info and music genre
// figure out three most potential affected genres
// columns used: artist latitude, artist longitude, track genres_all
// steps: first create a new column of location from artist latitude and artist longitude,
//        secondly, explode the column track genres_all into rows of distinct genre id 
//        thirdly, group all the location of the same genre id into a new column called locations
//        finally, output the new dataframe of genre id and locations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("multiline", "true").load("Project/Data/tracks.csv")
// Step 1: Create a new column for location from latitude and longitude formatted as (latitude, longitude)
val dfWithLocation = df.withColumn("location", concat(lit("("), col("artist latitude"), lit(","), col("artist longitude"), lit(")")))
// Define a UDF to check if a location is valid (numeric pair)
val isValidLocation = udf((location: String) => {
  try {
    val Array(lat, lon) = location.stripPrefix("(").stripSuffix(")").split(",")
    lat.toDouble
    lon.toDouble
    true
  } catch {
    case _: Throwable => false
  }
})
// Filter out rows where location is not a valid numeric pair
val filteredDF = dfWithLocation.withColumn("isValidLocation", isValidLocation(col("location"))).filter(col("isValidLocation"))
// Drop the temporary column isValidLocation
val cleanedDF = filteredDF.drop("isValidLocation")
// Step 2: Explode the track genres_all column
// Define a UDF to extract and validate genre IDs
val extractAndValidateGenres = udf((genreList: String) => {
  if (genreList == null || genreList.isEmpty) {
    Array.empty[Int]  // Return empty array for null or empty strings
  } else {
    try {
      // Remove square brackets and split by comma (and optional space)
      val genres = genreList.stripPrefix("[").stripSuffix("]").split(",\\s*")
      // Filter out non-numeric strings and convert to integer array
      genres.filter(_.forall(_.isDigit)).map(_.toInt)
    } catch {
      case _: Throwable => Array.empty[Int]  // Handle any conversion errors
    }
  }
})
// Apply the UDF to extract and validate genre IDs
val explodedDF = cleanedDF.withColumn("genre id", explode(extractAndValidateGenres(col("track genres_all"))))
// Define a UDF to check if a genre ID is an integer
val isIntegerGenreId = udf((genreId: String) => {
  try {
    genreId.toInt
    true
  } catch {
    case _: Throwable => false
  }
})
// Filter out rows where genre_id is not an integer
val filteredDF = explodedDF.withColumn("isIntegerGenreId", isIntegerGenreId(col("genre id"))).filter(col("isIntegerGenreId"))
// Drop the temporary column isIntegerGenreId
val cleanedDF = filteredDF.drop("isIntegerGenreId")
// Step 3: Group by genre_id and aggregate locations into a new column
val genreLocationDF = cleanedDF.groupBy("genre id").agg(collect_set("location").alias("locations")).filter(size(col("locations")) > 0) // Filter out rows where locations array is empty
// Show the DataFrame with genre_id and locations
genreLocationDF.show(false)
// save to hdfs
// Convert DataFrame to an array of strings
val stringArray = genreLocationDF.collect().map(_.mkString(","))
val stringRDD = spark.sparkContext.parallelize(stringArray)
stringRDD.coalesce(1).saveAsTextFile("Project/tracks_ana_geolocation")
// 2+. create a picture showing the most geographically corelated genre's geographical distribution



