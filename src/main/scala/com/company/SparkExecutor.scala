package com.company

import com.company.model._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.concurrent.{ExecutionContext, Future}
class SparkExecutor(implicit executionContext: ExecutionContext) {
  def testCsv(spark: SparkSession): Future[String] = Future {
    import MyEncoders._
    import spark.implicits._
    import org.apache.spark.sql.SQLImplicits

    implicit def bodyEncoder: ExpressionEncoder[Body] = EncodersBuilder.encoder4(Body.apply)
    implicit def secondEncoder: ExpressionEncoder[Second] = EncodersBuilder.encoder2(Second.apply)

    val body = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/sample.csv")
      .as[BodyWorkpiece]

//    body.show()

    val color = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/color.csv")
      .as[ColorWorkpiece]

//    color.show()

    val material = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/material.csv")
      .as[MaterialWorkpiece]

//    material.show()

    val b = body
      .join(color, body.col("colorId") === color.col("id"))
      .join(material, body.col("materialId") === material.col("id"))
      .select(body.col("id"), body.col("width"), material.col("material"), color.col("color"))
      .as[Body]
//    b.show()

    val second = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/second.csv")
      .as[Second]

    b.join(second, b.col("material") === second.col("material")).show()

    b.collect().toList.toString()
//    "hi"
  }

  def calculateWords(spark: SparkSession) : Future[String] = Future {
    import spark.implicits._
    import org.apache.spark.sql.functions._
//    val stringTokenizer = new StringTokenizer()
    //    spark.sparkContext.tex
    val splitter = Array(';', ',',' ', '-', ':', '.')
    val input = spark.read.option("headed", "false").csv("src/main/resources/date.txt").as[String]
    val res = input.flatMap(_.split(splitter)).filter(_ != "").map(_.length).groupBy($"value")
      .agg(count("*") as "numOccurances").orderBy($"value" desc)
      .map(row => Answer(row.getAs[Int]("value"), row.getAs[Long]("numOccurances")))
    res.show()
    val results = res.collect().toSeq

    val outlayers = input.flatMap(_.split(splitter)).filter(_.length > 16)
    outlayers.show()
    "a"
  }
}

case class Answer(value: Int, count: Long)
