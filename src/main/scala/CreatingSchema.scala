package Basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import java.sql.Date
object CreatingSchema {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().master("local").appName("creatingSchema").getOrCreate()

    val nameDf =spark.read.option("header","true").option("inferSchema","true")
      .csv("src/main/resources/matches.csv")

    nameDf.printSchema()

    val ownSchema= StructType(
      StructField("id",IntegerType,true)::
        StructField("season",IntegerType,true)::
        StructField("city",StringType,true)::
        StructField("date",DateType,true)::
        StructField("team1",StringType,true)::
        StructField("team2",StringType,true)::
        StructField("toss_winner",StringType,true)::
        StructField("toss_decision",StringType,true)::
        StructField("result",StringType,true)::
        StructField("dl_applied",IntegerType,true)::
        StructField("winner",StringType,true)::
        StructField("win_by_runs",IntegerType,true)::
        StructField("win_by_wickets",StringType,true)::
        StructField("player_of_match",StringType,true)::
        StructField("venue",StringType,true)::
        StructField("umpire1",StringType,true)::
        StructField("umpire2",StringType,true)::
        StructField("umpire3",StringType,true)::Nil)

    val namesWithOwnSchema = spark.read.option("header","true").schema(ownSchema).csv("src/main/resources/matches.csv")

    namesWithOwnSchema.printSchema()

  }
}
