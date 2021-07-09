package Basics

import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object CreatingDFWithSC {
  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf()
      .setMaster("local").setAppName("Creating DataFrame")

    val sc =  new SparkContext(sparkConf)

    val sqlContext= new SQLContext(sc)

    val RDD = sc.parallelize(Array(1,2,3,4),3)

    val schema = StructType(
      StructField("Numbers",IntegerType,true):: Nil
    )
    val rowRDD = RDD.map(line=>Row(line))

    val DF = sqlContext.createDataFrame(rowRDD,schema)

    DF.printSchema()
    DF.show()
  }
}
