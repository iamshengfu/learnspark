package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object SparkSchemaDemo extends Serializable {
  def main(args: Array[String]) = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if (args.length == 0) {
      logger.info("Usage: HelloRDD filename")
      System.exit(1)
    }

    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))

    val spark = SparkSession.builder()
      .appName("Hellow SparkSQL")
      .master("local[3]")
      .getOrCreate()

    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header","true")
      .option("path","data/flight-time.csv")
      //.option("inferSchema","true")
      .schema(flightSchemaStruct)
      .option("dateFormat","M/d/y")
      .load()

    val flightTimeJsonDF = spark.read
      .format("json")
      .option("path","data/flight-time.json")
      .schema(flightSchemaStruct)
      .option("dateFormat","M/d/y")
      .load()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path","data/flight-time.parquet")
      .load()


    flightTimeCsvDF.show(5)
    logger.info("Csv Schema: " + flightTimeCsvDF.schema.simpleString)
    logger.info("Json Schema: " + flightTimeJsonDF.schema.simpleString)
    spark.stop()
  }
}
