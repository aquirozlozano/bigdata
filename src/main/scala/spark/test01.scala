package spark

import utils.SparkBase

object test01 {

  val spark = SparkBase.getSparkSession()
  def main(args: Array[String]): Unit = {


    spark.sparkContext.getConf.getAll.foreach(println)

  }
}
