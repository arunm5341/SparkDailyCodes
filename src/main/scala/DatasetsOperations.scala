import org.apache.spark.sql.SparkSession

import java.sql.Date

object DatasetsOperations {
  case class Matches(id:Integer,season :Integer, city:String,date:Date,team1:String,team2:String,toss_winner:String,
                     toss_decision:String,result:String,dl_applied:Integer,winner:String,win_by_runs:Integer,
                     win_by_wickets:Integer,player_of_match:String,venue:String,umpire1:String,umpire2:String,umpire3:String)

  def main(args: Array[String]): Unit = {

    val sparkSession=SparkSession.builder()
      .master("local")
      .appName("CreateDatasets")
      .getOrCreate()

    import sparkSession.implicits._

    val DataSets= sparkSession.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/matches.csv")
      .as[Matches]

    val matches_RCB = DataSets.filter( MatchesObj => MatchesObj.team1=="Royal Challengers Bangalore")
    matches_RCB.show()

    val RCB=DataSets.where("winner== 'Royal Challengers Bangalore'")
    RCB.show()


}
}

