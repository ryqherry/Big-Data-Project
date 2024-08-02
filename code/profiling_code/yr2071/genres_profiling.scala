import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("genre_id", IntegerType, nullable = true),StructField("#tracks", IntegerType, nullable = true),StructField("parent", IntegerType, nullable = true),StructField("title", StringType, nullable = true),StructField("top_level", IntegerType, nullable = true)))
val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "false").schema(schema).load("Project/Data/genres.csv")
val numRows = df.count()
val topGneres = df.filter($"parent" === 0)
val numDistinctTopGenres = topGneres.count()