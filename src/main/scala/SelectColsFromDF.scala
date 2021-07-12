package Basics2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SelectColsFromDF {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local").appName("Select columns from dataFrame").getOrCreate()

    val data = Seq(("arun","mallik",23,"India"),
      ("eniya","kumar",23,"India"),
      ("kashif","ahmed",23,"India"),
      ("mithlesh","chaurasiya",25,"India"))

    val columns = Seq("First_name","Last_name","Age","Country")

    import spark.implicits._

    val df= data.toDF(columns:_*)
    df.show()

    // * selecting columns from data frame
    df.select("First_name","Last_name").show()

    // * Using Dataframe object name
    df.select(df("First_name"),df("Last_name")).show()

    // * Using col function
    df.select(col ("First_name"),col("Last_name")).show()

    // * show all columns
    df.select("*").show()

    //show columns from list
    val listCols= List("Last_name","Country")
    df.select(listCols.map(m=>col(m)):_*).show()

    //Show first few columns and one row
    df.select(df.columns.slice(0,3).map(m=>col(m)):_*).show(1)

  }
}
