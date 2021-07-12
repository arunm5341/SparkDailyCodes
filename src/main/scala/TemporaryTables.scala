package Basics

import org.apache.spark.sql.SparkSession

object TemporaryTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("CreatingDFWithSS").getOrCreate()

    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/matches.csv")

    df.registerTempTable("limitedRow")
    spark.sql("select * from limitedRow limit 10").show()
  }
}
