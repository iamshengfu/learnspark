package shengfu

import java.sql.Date
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import shengfu.RowDemo.toDateDF

case class myRow(ID: String, EventDate: Date)

class RowDemoTest extends FunSuite with BeforeAndAfterAll{

  @transient var spark: SparkSession = _
  @transient var myDF: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Demo row test")
      .master("local[3]")
      .getOrCreate()

    val mySchema = StructType(List(
      StructField("ID",StringType),
      StructField("EventDate",StringType)
    ))

    val myRows = List(Row("123","04/05/2020"),Row("124","4/5/2020"),Row("125","04/5/2020"),Row("125","4/05/2020"))
    val myRDD = spark.sparkContext.parallelize(myRows,2)
    myDF = spark.createDataFrame(myRDD,mySchema)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Test data type") {
    toDateDF(myDF,"M/d/y","EventDate").collect().foreach(
      row => assert(row.get(1).isInstanceOf[Date],"Second column should be date")
    )
  }

  test("Test data value") {
    val spark2 = spark
    import spark2.implicits._
    toDateDF(myDF,"M/d/y","EventDate").as[myRow].collect().foreach(row =>
      assert(row.EventDate.toString == "2020-04-05","Date string must be 2020-04-05")
    )
  }

}
