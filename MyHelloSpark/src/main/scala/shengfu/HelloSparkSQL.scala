package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import shengfu.HelloDataSet.getClass

object HelloSparkSQL extends Serializable {
  def main(args: Array[String]) = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if (args.length == 0) {
      logger.info("Usage: HelloRDD filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Hellow SparkSQL")
      .master("local[3]")
      .getOrCreate()

    val rawDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(args(0))

    rawDF.createOrReplaceTempView("survey_tb1")
    val countDF = spark.sql("select Country, count(1) as count from survey_tb1 where Age < 40 group by Country")
    logger.info(countDF.collect().mkString("->"))
    spark.stop()
  }
}
