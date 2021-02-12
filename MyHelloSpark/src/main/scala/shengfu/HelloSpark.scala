package shengfu

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    val surveyDF = loadSurveyDF(spark, args(0))
    val partitionedSurveyDF = surveyDF.repartition(2)
    val df = countByCountry(partitionedSurveyDF)

    logger.info(df.collect().mkString("->"))
    spark.stop()
  }

  def countByCountry(inputDF: DataFrame): DataFrame = {
    inputDF.where("Age < 40")
      .select("Age","Gender","Country","state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, dataFile: String):DataFrame = {
    spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(dataFile)
  }

  def getSparkConf: SparkConf = {
    val sparkAppConf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k,v) => sparkAppConf.set(k.toString,v.toString))
    sparkAppConf
  }
}
