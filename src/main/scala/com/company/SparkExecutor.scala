package com.company

import com.company.model.Body
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.concurrent.{ExecutionContext, Future}

class SparkExecutor(implicit executionContext: ExecutionContext) {
  def testCsv(spark: SparkSession): Future[String] = Future {
    implicit def bodyEncoder: ExpressionEncoder[Body] = MyEncoders.caseClass(Seq(
      Encoders.INT.asInstanceOf[ExpressionEncoder[Int]],
      Encoders.DOUBLE.asInstanceOf[ExpressionEncoder[Double]],
      MyEncoders.materialEncoder,
      MyEncoders.colorEncoder
    ))

    val ds = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/sample.csv")
      .as[Body]
    ds.show(false)

    ds.collect().toList.toString()
  }
}
