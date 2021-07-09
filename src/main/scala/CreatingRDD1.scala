package Basics

import org.apache.spark.{SparkConf, SparkContext}

object CreatingRDD1 {
  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf().setAppName("Creating RDD").setMaster("local")

    val sc = new SparkContext(sparkConf)

    val array = Array(1,2,3,4,5,6)

    val arrayRDD = sc.parallelize(array,2)
    array.foreach(println)

    val file= "src/main/resources/matches.csv"
    val fileRDD = sc.textFile(file,5)
    println(fileRDD.count())
  }
}
