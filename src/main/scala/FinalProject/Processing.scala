package FinalProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Processing {

  def getTwoDFofBooks(filepath1: String, filePath2: String, spark: SparkSession): Unit = {
    println(s"Opening TXT at $filePath2")
    //
    import spark.implicits._

    val bookstxt = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("charset", "utf-16")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .load(filePath2)

    val bookstxtDF = bookstxt.toDF
    bookstxtDF.show()

    println(s"Opening CSV at $filepath1")

    val goodReadsBooksDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filepath1)

    goodReadsBooksDF.show()
    goodReadsBooksDF.filter($"average_rating"  > 4.9).show()

    goodReadsBooksDF.filter($"language_code" === "jpn" && $"average_rating" > 4.3).show()

    val languages = goodReadsBooksDF.groupBy("language_code").count()
    languages.orderBy(desc("count")).take(6).foreach(println)

    val newgoodReads = goodReadsBooksDF.withColumn("  num_pages", when(col("  num_pages") === "eng", "100")
      .otherwise(col("  num_pages"))
    )

    val manyPages = newgoodReads.orderBy(desc("  num_pages")).take(1)


    println(s"The most thick book in given dataset was ${manyPages.mkString}")


    val mergedDF = bookstxtDF.as("d1").join(goodReadsBooksDF.as("d2"),
      $"d1.title" === $"d2.title")
      .select($"d1.*", $"d2.average_rating")

    mergedDF.show()
    mergedDF(colName = "average_rating").desc
    mergedDF.orderBy($"average_rating".desc).show
    mergedDF.agg(min("average_rating"), max("average_rating")).show()
    val highest_score = mergedDF.orderBy(desc("average_rating")).take(1).mkString
    println(s"Highest score from most sold books was $highest_score ")


    mergedDF.write.mode("overwrite")
      .json("c:/temp/booksbooks.json")
  }


}
