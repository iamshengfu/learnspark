package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import shengfu.HelloSpark.getSparkConf

object SparkSQLTableDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path","data/flight-time.parquet")
      .load()

    spark.sql("Create database if not exists AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write
      .mode(SaveMode.Overwrite)
      //.partitionBy("ORIGIN", "OP_CARRIER")
      .bucketBy(5,"ORIGIN","OP_CARRIER")
      .sortBy("ORIGIN","OP_CARRIER")
      .saveAsTable("flight_data_tbl")

    spark.catalog.listTables("AIRLINE_DB").show()

    logger.info("Finished")
    spark.stop()
  }
}
