package utils

import org.apache.hadoop.hdfs.DFSClient.Conf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBase {

  def getSparkSession(appName:String="Rcc"): SparkSession = {

    val conf= new SparkConf()
      .setAppName(appName)
      .set("spark.ui.enabled","true")
      .setMaster("local")

    val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark

  }

}
