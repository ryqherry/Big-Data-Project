import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("genre_id", IntegerType, nullable = true),StructField("#tracks", IntegerType, nullable = true),StructField("parent", IntegerType, nullable = true),StructField("title", StringType, nullable = true),StructField("top_level", IntegerType, nullable = true)))
val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "false").schema(schema).load("Project/Data/genres.csv")
val mean_num = df.select(mean(col("#tracks"))).as[Double].first()
val median_num = df.stat.approxQuantile("#tracks", Array(0.5), 0.01)(0)
val stddev_num = df.select(stddev(col("#tracks"))).as[Double].first()
println(s"Mean of #tracks: $mean_num")
println(s"Median of #tracks: $median_num")
println(s"Standard deviation of #tracks: $stddev_num")