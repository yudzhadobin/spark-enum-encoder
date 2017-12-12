package com.company

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.company.model.Body
import com.company.model.Colors.{Blue, Red}
import com.company.model.Materials.{Glass, Metal, Wood}
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

    ds.filter(_.material == Glass).show(false)
    ds.filter(_.color == Red).show(false)
    ds
      .filter(_.id > 1)
      .filter(_.color != Blue)
      .collect().toList.toString()
  }

  def testLocalDateTime(spark: SparkSession): Future[String] = Future {
    import com.company.MyEncoders._
    import spark.implicits._
    val ds = Seq(
      LocalDateTime.now().plus(1, ChronoUnit.DAYS),
      LocalDateTime.now().minus(1, ChronoUnit.MINUTES),
      LocalDateTime.now().plus(2, ChronoUnit.DAYS),
      LocalDateTime.now()
    ).toDS().filter(_.isBefore(LocalDateTime.now()))
    ds.show(false)
    ds.toString()

    val dss = Seq(
      "2017-01-01T01:01:00.000",
      "2017-01-01T02:02:00.000",
      "2017-01-01T02:02:00.000"
    ).toDS().as[LocalDateTime].filter(_.isBefore(LocalDateTime.parse("2017-01-01T02:02:00.000")))

    dss.show()
    dss.collect().toList.toString()
  }
}
