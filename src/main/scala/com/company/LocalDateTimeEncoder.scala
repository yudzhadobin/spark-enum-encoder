package com.company

import java.time.LocalDateTime

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect._

object LocalDateTimeEncoder {
  val clazz = classOf[java.time.LocalDateTime]
  val inputObject = BoundReference(0, ObjectType(clazz), nullable = true)

  // call UTF8String.fromString(<instance>.toString) in Expression way
  val converter = StaticInvoke(
    classOf[UTF8String],
    StringType,
    "fromString",
    Invoke(inputObject, "toString", ObjectType(classOf[String])) :: Nil
  )

  val serializer = CreateNamedStruct(Literal("value") :: converter :: Nil)

  val deserializer: Expression = StaticInvoke(
    DateTimeUtils.getClass,
    ObjectType(classOf[LocalDateTime]),
    "toJavaTimestamp",
    Literal("value") :: Nil)

  implicit def scalaLocalDateTime: Encoder[LocalDateTime] =
    new ExpressionEncoder[LocalDateTime](
      serializer.dataType,
      flat = false,
      serializer.flatten,
      deserializer,
      classTag[java.time.LocalDateTime])
}
