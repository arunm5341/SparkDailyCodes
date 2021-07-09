package Basics

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreatingDFWithCSVFile {
  def main(args: Array[String]): Unit = {
    var sparkConf= new SparkConf().setMaster("local").setAppName("Creating Dataframe")

    val sc = new SparkContext(sparkConf)

    val sqlContext= new SQLContext(sc)

    val df= sqlContext.read.option("header","true").option("inferSchema","true").format("matches.csv")
      .load("src/main/resources/matches.csv")

    df.printSchema()
    df.show()
  }

}
