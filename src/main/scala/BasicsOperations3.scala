package Basics

import org.apache.spark.sql.SparkSession

object BasicsOperations3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("CreatingDFWithSS").getOrCreate()

    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/matches.csv")

    // Selecting the columns in data frame

    df.select("winner","umpire1").where("winner=='Royal Challengers Bangalore'").show(3)

    // GroupBy with Count

    val winners =df.select("winner").groupBy("winner").count().show()
    
  }
}
