package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//case class SurveyRecord(Age: Int, Gender: String, Country: String, state:String)

object HelloDataSet extends  Serializable {

  def main(args: Array[String]) = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if(args.length == 0) {
      logger.info("Usage: HelloRDD filename")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Hellow DataSet")
      .master("local[3]")
      .getOrCreate()

    val rawDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(args(0))

    //import spark.implicits._
    val surveyDS= rawDF.select("Age","Gender","Country","state")

    val filtered = surveyDS.filter("Age < 40")
    val countDS = filtered.groupBy("Country").count()

    logger.info("DataSet:" + countDS.collect().mkString(","))
    spark.stop()
  }
}
