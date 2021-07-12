package Basics

import org.apache.spark.sql.SparkSession

object TemporaryTables2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("CreatingDFWithSS").getOrCreate()

    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/matches.csv")

    df.createTempView("TempTables")

    val RCB= spark.sql("select * from TempTables limit 10").show()

   // RCB.createOrReplaceTempView("TempTables")
    spark.sql("select * from TempTables where winner='Royal Challengers Bangalore'").show()



    //spark.sql("select * from RCB limit 5")
  }
}
