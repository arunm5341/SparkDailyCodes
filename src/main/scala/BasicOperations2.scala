package Basics

import org.apache.spark.sql.SparkSession

object BasicOperations2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("CreatingDFWithSS").getOrCreate()

    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/matches.csv")

    // Describe column

    val matches_description = df.describe("winner").show()

    // Describe each column and its types

    val colAndTypes = df.dtypes
    colAndTypes.foreach(println)

    // Print the first top 5 lines

    val head=df.head(5)
    .foreach(println)

  }
}
