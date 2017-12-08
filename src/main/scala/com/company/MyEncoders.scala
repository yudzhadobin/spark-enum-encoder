package com.company

import com.company.model.Material
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

import scala.reflect._

object MyEncoders {
  implicit val materialEncoder: Encoder[Material] = ExpressionEncoder(
    StructType(StructField("value", StringType) :: Nil),
    flat = true,
    Seq(),
    ScalaReflection.deserializerFor[Material],
    classTag[Material]
  )
}