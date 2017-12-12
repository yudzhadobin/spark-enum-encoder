package com.company

import com.company.model.Body
import com.company.model.Colors._
import com.company.model.Materials._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class CaseClassEncoderTest extends WordSpec with Matchers with BeforeAndAfter {

  private val master = "local[2]"
  private val app = "spark-test-encoders"

  private var spark: SparkSession = _

  implicit def bodyEncoder: ExpressionEncoder[Body] = MyEncoders.caseClass(Seq(
    Encoders.INT.asInstanceOf[ExpressionEncoder[Int]],
    Encoders.DOUBLE.asInstanceOf[ExpressionEncoder[Double]],
    MyEncoders.materialEncoder,
    MyEncoders.colorEncoder
  ))

  before {
    spark = SparkSession.builder
      .appName(app)
      .config("spark.master", master)
      .getOrCreate()
  }

  after {
    if (spark != null) spark.stop()
  }

  "case class should be castable to DS" in {
    val ss = spark
    import ss.implicits._

    val ds = Seq(
      Body(1, 1.0, Metal, Blue),
      Body(2, 2.0, Glass, Red),
      Body(3, 3.0, Wood, Green),
      Body(4, 4.0, Glass, Blue)
    ).toDS()
    //ds.show()
    ds.collect().toSeq should be (Seq(
      Body(1, 1.0, Metal, Blue),
      Body(2, 2.0, Glass, Red),
      Body(3, 3.0, Wood, Green),
      Body(4, 4.0, Glass, Blue)
    ))
  }

  "case class should be filterable to proof SerDe" in {
    val ds = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/sample.csv")
      .as[Body]
    //ds.show()
    ds.filter(_.color != Blue)
      .filter(_.material == Glass)
      .collect().toSeq should be (Seq(
      Body(2, 2.0, Glass, Green)
    ))
  }

  "case class can be built from csv" in {
    val ds = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/sample.csv")
      .as[Body]
    //ds.show()
    ds.collect().toSeq should be (Seq(
      Body(1, 1.0, Wood, Red),
      Body(2, 2.0, Glass, Green),
      Body(3, 3.0, Metal, Blue)
    ))
  }

  "case class should fail if meet incorrect value" in {
    val ds = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/incorrect.csv")
      .as[Body]
    //ds.show()
    val t = the[RuntimeException] thrownBy { ds.collect() }
    t.getCause.getMessage should be ("'glazz' not a Material")
  }

  "case class should propagate null" in {
    val ds = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/withNull.csv")
      .as[Body]
    //ds.show()
    ds.collect().toSeq should be (Seq(
      Body(1, 1.0, Wood, Red),
      Body(2, 2.0, null, Green),
      Body(3, 3.0, Metal, null)
    ))
  }

  "case class loop test" in {
    val ss = spark
    import ss.implicits._

    val ds = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/test/resources/sample.csv")
      .as[Body]
    //ds.show()
    ds
      .collect()
      .toSeq
      .toDS
      .filter(_.id > 0)
      .collect() should be (Seq(
      Body(1, 1.0, Wood, Red),
      Body(2, 2.0, Glass, Green),
      Body(3, 3.0, Metal, Blue)
    ))
  }
}
