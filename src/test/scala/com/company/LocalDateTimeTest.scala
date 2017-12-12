package com.company

import java.time.LocalDateTime
import java.time.format.DateTimeParseException

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class LocalDateTimeTest extends WordSpec with Matchers with BeforeAndAfter {
  private val master = "local[2]"
  private val app = "spark-test-encoders"

  private var spark: SparkSession = _

  before {
    spark = SparkSession.builder
      .appName(app)
      .config("spark.master", master)
      .getOrCreate()
  }

  after {
    if(spark != null) spark.stop()
  }

  val sample = Seq(
    LocalDateTime.parse("2017-01-01T01:01:01.001"),
    LocalDateTime.parse("2017-02-02T02:02:02.002"),
    LocalDateTime.parse("2017-01-03T03:03:03.003")
  )

  "LocalDateTime should be castable to DS" in {
    val ss = spark
    import MyEncoders.scalaLocalDateTime
    import ss.implicits._

    sample.toDS().collect().toSeq should be (sample)
  }

  "LocalDateTime can be built from string" in {
    val ss = spark
    import MyEncoders.scalaLocalDateTime
    import ss.implicits._

    val ds = Seq(
      "2017-01-01T01:01:01.001",
      "2017-02-02T02:02:02.002",
      "2017-01-03T03:03:03.003"
    ).toDS().as[LocalDateTime]

    ds.collect().toSeq should be (sample)
  }

  "LocalDateTime should be filterable to proof SerDe" in {
    val ss = spark
    import MyEncoders.scalaLocalDateTime
    import ss.implicits._

    val ds = sample.toDS().as[LocalDateTime].filter(_.isAfter(LocalDateTime.parse("2017-01-02T01:01:01.001")))

    ds.collect().toSeq should be (Seq(
      LocalDateTime.parse("2017-02-02T02:02:02.002"),
      LocalDateTime.parse("2017-01-03T03:03:03.003")
    ))
  }

  "LocalDateTime should fail if value is incorrect" in {
    val ss = spark
    import MyEncoders.scalaLocalDateTime
    import ss.implicits._

    a[DateTimeParseException] should be thrownBy {
      Seq(
        LocalDateTime.parse("2017-01-01T01:01:01.001"),
        LocalDateTime.parse("2017-02-02 02"),
        LocalDateTime.parse("2017-01-03T03:03:03.003")
      ).toDS().collect()
    }
  }
}
