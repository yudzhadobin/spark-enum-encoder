package com.company

import com.company.model.Materials.{Glass, Metal, Wood}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import com.company.MyEncoders._
import com.company.model.Material

class EnumEncoderTest extends WordSpec with Matchers with BeforeAndAfter {

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

  "enum should be castable to DS" in {
    val ss = spark
    import ss.implicits._

    val ds = Seq(
      Glass,
      Wood,
      Glass,
      Metal
    ).toDS()
    //ds.show()
    ds.collect().toSet should be (Set(Glass, Wood, Metal))
  }

  "enum should be filterable to proof SerDe" in {
    val ss = spark
    import ss.implicits._

    val ds = Seq(
      Glass,
      Wood,
      Glass,
      Metal
    ).toDS().filter(_ != Glass)
    //ds.show()
    ds.collect().toSet should be (Set(Wood, Metal))
  }

  "enum can be built from strings" in {
    val ss = spark
    import ss.implicits._

    val ds = Seq(
      "glass",
      "wood",
      "glass",
      "metal"
    ).toDS.as[Material]
    //ds.show()
    ds.collect().toSet should be (Set(Glass, Wood, Metal))
  }

  "encoder should fail if meet incorrect value" in {
    val ss = spark
    import ss.implicits._

    val ds = Seq(
      "glass",
      "wood",
      "glassssss",
      "metal"
    ).toDS.as[Material]
    //ds.show()
    val t = the[RuntimeException] thrownBy { ds.collect() }
    t.getCause.getMessage should be ("'glassssss' not a Material")
  }

  "encoder should propagate null" in { // or it shouldn't - then fix enum implementation
    val ss = spark
    import ss.implicits._

    val ds = Seq(
      "glass",
      "wood",
      null,
      "metal"
    ).toDS.as[Material]
    //ds.show()
    ds.collect().toList should be (List(Glass, Wood, null, Metal))
  }
}
