package Basics2

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object AddNewColToDF {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().master("local").appName("Select columns from dataFrame").getOrCreate()

    val data = Seq(Row("arun","mallik",23,"India"),
      Row("eniya","kumar",23,"India"),
      Row("kashif","ahmed",23,"India"),
      Row("mithlesh","chaurasiya",25,"India"))

    val sch1= new StructType()
      .add("First_name",StringType)
      .add("Last_name",StringType)
      .add("age",IntegerType)
      .add("Country",StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),sch1)

    //Change the column type
    df.withColumn("age",df("age").cast("Long")).show()

    // Transforming the existing column
    df.withColumn("age",df("age")+10).show()

    // Renaming a column
    df.withColumnRenamed("Last_name","Name_Last").show()

    //Droping a column
    //val df6=df4.drop("CopiedColumn")
    //println(df6.columns.contains("CopiedColumn"))


  }
}
