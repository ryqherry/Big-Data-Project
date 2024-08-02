import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

val df: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Project/Data/echonest.csv")
df.show(5)