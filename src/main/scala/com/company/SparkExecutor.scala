package com.company

import java.time.LocalDateTime

import com.company.model.Body
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.concurrent.{ExecutionContext, Future}

class SparkExecutor(implicit executionContext: ExecutionContext) {
  def testCsv(spark: SparkSession): Future[String] = Future {
    import spark.implicits._
    import com.company.MyEncoders._

    val schema = StructType
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/sample.csv")
      .as[Body]

    val result = data
    result.show()
    s"total: ${result.count()} of ${result.toString()}"
  }

  def testLocalDateTime(spark: SparkSession): Future[String] = Future {
    import com.company.MyEncoders._
    import spark.implicits._
    val ds = Seq(LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now()).toDS()
      .as[String].cache()
      .as[LocalDateTime].cache()
    ds.show()
    ds.toString()
  }
}
