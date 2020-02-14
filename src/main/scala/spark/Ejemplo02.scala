package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SparkBase

object Ejemplo02 {

  private val spark = SparkBase.getSparkSession()

  def main(args: Array[String]): Unit = {
    val path ="C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\data\\EasyWeather.txt"

    val destiPath ="C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\dataOut\\ejemplo02"

   readFileCsv(spark,path).show()

    val df = readFileCsv(spark,path)

    saveFileAsParquet(destiPath,df)
  }

  def readFileCsv(spark: SparkSession,path:String): DataFrame ={
   val datosMeteorologicos= spark.read.format("csv").option("header",true).option("delimiter","\t").load(path)
    datosMeteorologicos
  }

  def saveFileAsParquet(destiPath:String,df:DataFrame):Unit ={
  df.coalesce(1).write.format("parquet").mode("overwrite").save(destiPath)
  }

  def readFileParquet(spark: SparkSession,path:String): DataFrame ={
    val datosMeteorologicos= spark.read.format("parquet").load(path)
    datosMeteorologicos
  }


}
