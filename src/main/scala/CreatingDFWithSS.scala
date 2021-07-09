package Basics

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object CreatingDFWithSS {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local").appName("CreatingDFWithSS").getOrCreate()

    val RDD = spark.sparkContext.parallelize(Array(1, 1, 2, 23, 3), 3)

    val schema = StructType(
      StructField("Integers as String ", IntegerType, true) :: Nil
    )

    val rowRDD = RDD.map(x => Row(x))

    val DF = spark.createDataFrame(rowRDD, schema)

    DF.printSchema()
    DF.show()

  }
}
