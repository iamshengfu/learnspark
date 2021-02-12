package shengfu

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Misc Demo")
      .master("local[3]")
      .getOrCreate()

    val invoiceDF = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("data/invoices.csv")
    /*
        invoiceDF.select(
          count("*").as("Count all"),
          sum("Quantity").as("Total Quantity"),
          avg("UnitPrice").as("AvgPrice"),
          countDistinct("InvoiceNo").as("CountDistinct")
        ).show()

        invoiceDF.selectExpr(
          "count(1) as `count 1`",
          "count(StockCode) as `count field`",
          "sum(Quantity) as TotalQuantity",
          "avg(UnitPrice) as AvgPrice"
        ).show()


    invoiceDF.createOrReplaceTempView("Sales")
    val summarySQL = spark.sql(
      """
        |Select Country, InvoiceNo, sum(Quantity) as TotalQuantity, round(sum(Quantity * UnitPrice),2) as InvoiceValue
        |from Sales
        |Group by Country, InvoiceNo
        |""".stripMargin).show()

    val summaryDF = invoiceDF.groupBy("Country","InvoiceNo")
      .agg(sum("Quantity").as("TotalQuantity"),
        round(sum(expr("Quantity*UnitPrice")),2).as("InvoiceValue")
      ).show()

    invoiceDF.createOrReplaceTempView("Sales1")
    spark.sql(
      """
        |Select Country, weekofyear(to_date(InvoiceDate,'dd-MM-yyyy H.mm')) as WeekNumber, count(DISTINCT InvoiceNo) as NumInvoices, sum(Quantity) as TotalQuantity, round(sum(Quantity * UnitPrice),2) as InvoiceValue
        |from Sales1 where year(to_date(InvoiceDate,'dd-MM-yyyy H.mm')) == 2010
        |group by Country, WeekNumber
        |order by Country, WeekNumber
        |""".stripMargin).show()*/

    val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
    val TotalQuantity = sum("Quantity").as("TotalQuantity")
    val InvoiceValue = expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    val exSummaryDF = invoiceDF
      .withColumn("InvoiceDate", to_date(col("InvoiceDate"),"dd-MM-yyyy H.mm"))
      .where(expr("year(InvoiceDate) == 2010"))
      .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
      .groupBy("Country", "WeekNumber")
      .agg(NumInvoices, TotalQuantity, InvoiceValue)

    exSummaryDF.coalesce(1).write.format("parquet").mode("overwrite").save("output")
    exSummaryDF.sort("Country","WeekNumber").show()
    spark.stop()
  }
}
