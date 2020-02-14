package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SparkBase

object Ejemplo03 {

  val spark = SparkBase.getSparkSession()

  def main(args: Array[String]): Unit = {
    val path ="C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\data\\EasyWeather.txt"


    readFileCsv(spark,path).createOrReplaceTempView("temp")

    //spark.sql("select direction,count(*) from temp group by direction").show()

    val df =readFileCsv(spark,path)

    df.groupBy("direction").count().show()
  }

  def readFileCsv(spark: SparkSession,path:String): DataFrame ={
    val datosMeteorologicos= spark.read.format("csv").option("header",true).option("delimiter","\t").load(path)
    datosMeteorologicos
  }

}
