package com.company

import java.time.LocalDateTime
import java.time.temporal.{ChronoUnit, TemporalUnit}

import com.company.model.Materials.{Glass, Metal, Wood}
import com.company.model.{Body, Material}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.concurrent.{ExecutionContext, Future}

class SparkExecutor(implicit executionContext: ExecutionContext) {
  def testCsv(spark: SparkSession): Future[String] = Future {
    implicit def bodyEncoder: Encoder[(Int, Double, Material, String, LocalDateTime)] = ExpressionEncoder.tuple(
      Encoders.INT.asInstanceOf[ExpressionEncoder[Int]],
      Encoders.DOUBLE.asInstanceOf[ExpressionEncoder[Double]],
      MyEncoders.materialEncoder,
      Encoders.STRING.asInstanceOf[ExpressionEncoder[String]],
      MyEncoders.scalaLocalDateTime
    )

    val schema = StructType
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/sample.csv")
      .as[(Int, Double, Material, String, LocalDateTime)]
      .filter(t => t._3 == Glass)

    data.persist()

    val result = data
    result.show()
    s"total: ${result.count()} of ${result.toString()}"
  }

  def testEnum(spark: SparkSession): Future[String] = Future {
    import com.company.MyEncoders._
    import spark.implicits._
    val ds = Seq(
      Glass,
      Wood,
      Glass,
      Metal
    ).toDS().filter(_ == Glass)
    ds.show()
    ds.toString()
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
  }
}
