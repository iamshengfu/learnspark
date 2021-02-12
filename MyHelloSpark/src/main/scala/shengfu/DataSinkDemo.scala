package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import shengfu.HelloSpark.getSparkConf

object DataSinkDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path" , "data/flight-time.parquet")
      .load()
    logger.info(spark.version)
    logger.info(scala.util.Properties.scalaPropOrElse("version.number", "unknown"))
    logger.info("Number of partitions:" + flightTimeParquetDF.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.spark_partition_id
    flightTimeParquetDF.groupBy(spark_partition_id()).count.show()
    //val partitionedDF = flightTimeParquetDF.repartition(5)
    //logger.info("Number of partitions:" + flightTimeParquetDF.rdd.getNumPartitions)
    //partitionedDF.groupBy(spark_partition_id()).count.show()
    /*
    partitionedDF.write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .option("path","dataSink/avro/")
      .save()
*/
    flightTimeParquetDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .option("path","dataSink/json/")
      .partitionBy("OP_CARRIER","ORIGIN")
      .option("maxRecordsPerFile",10000)
      .save()

    logger.info("Finished Partitioning.")
    spark.stop()
  }
}
