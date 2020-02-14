package spark

import utils.SparkBase


object Ejemplo01 {

  val spark = SparkBase.getSparkSession()

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin");
    val path="C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\data\\EasyWeather.txt"

    val datosMeteorologicos= spark.read.format("csv").option("header",true).option("delimiter","\t").load(path)

    //datosMeteorologicos.show()
   datosMeteorologicos.printSchema()

    //datosMeteorologicos.write.format("parquet").mode("overwrite").save("C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\dataOut\\datosMeteorologicos")

    spark.read.format("parquet").load("C:\\Users\\aquir\\IdeaProjects\\sparkTest\\src\\main\\resources\\dataOut\\datosMeteorologicos").show()

    spark.read.format("parquet").load("/fit_datalake/fit_dda/datosmeteorologicos")
  }

}
