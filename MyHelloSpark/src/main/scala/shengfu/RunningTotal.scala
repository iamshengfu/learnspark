package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum

object RunningTotal extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[3]").appName("Running total").getOrCreate()

    val runningtotal = spark.read.format("parquet")
      .option("path","output").load()

    val runningTotalWindow = Window.partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val s = "ss"

    runningtotal.withColumn("Running Total", sum("InvoiceValue").over(runningTotalWindow)).show()
  }
}
