package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SparkBase


object Ejemplo04 {
  val spark = SparkBase.getSparkSession()
  def main(args: Array[String]): Unit = {
    val path ="C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\data\\EasyWeather.txt"

    readFileCsv(spark,path).printSchema()
    readFileCsv(spark,path).
      select("Outdoor_Humidity","Indoor_Temperature","Indoor_Humidity")
      .filter("Outdoor_Humidity='99'").show()

    readFileCsv(spark,path).selectExpr("(case when Outdoor_Humidity ='99' then 1 else 0 end) test"
      ,"Indoor_Temperature").show()

  }
  def readFileCsv(spark: SparkSession,path:String): DataFrame ={
    val datosMeteorologicos= spark.read.format("csv").option("header",true).option("delimiter","\t").load(path)
    datosMeteorologicos
  }

}
