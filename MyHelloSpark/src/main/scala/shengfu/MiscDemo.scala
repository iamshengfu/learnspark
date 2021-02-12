package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object MiscDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val datalist = List(
      ("Ravi","28","1","2002"),
      ("Abdul","23","5","81"),
      ("John","12","12","6"),
      ("Rosy","7","8","63"),
      ("Abdul","28","1","81")
    )

    val rawDF = spark.createDataFrame(datalist).toDF("name","day","month","year").repartition(3)
    import org.apache.spark.sql.functions._

    val finalDF = rawDF.withColumn("id",monotonically_increasing_id )
      .withColumn("year",expr(
        """
          case when year < 21 then cast(year as int) + 2000
          when year < 100 then cast(year as int) + 1900
          else year
          end
          """
      ))
      .withColumn("dob",expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))
      .drop("day","month","year")
      .dropDuplicates("name")
      .sort(expr("dob desc"))

    finalDF.show()
  }

}
