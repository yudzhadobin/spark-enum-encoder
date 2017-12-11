package com.company

import java.time.LocalDateTime

import com.company.model.{Body, Material}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect._

object MyEncoders {

  implicit def materialEncoder: ExpressionEncoder[Material] = {
    val clazz = classOf[Material]
    val inputObject = BoundReference(0, ObjectType(clazz), nullable = true)

    // call UTF8String.fromString(<instance>.toString) in Expression way
    val converter = StaticInvoke(
      classOf[UTF8String],
      StringType,
      "fromString",
      Invoke(inputObject, "toString", ObjectType(classOf[String])) :: Nil
    )

    val serializer = CreateNamedStruct(Literal("material") :: converter :: Nil)

    val deserializer: Expression = StaticInvoke(
      clazz,
      ObjectType(clazz),
      "valueOf",
      Literal("material") :: Nil)

    new ExpressionEncoder[Material](
      serializer.dataType,
      flat = false,
      serializer.flatten,
      deserializer,
      classTag[Material]
    )
  }

  implicit def scalaLocalDateTime: ExpressionEncoder[LocalDateTime] = {
    val clazz = classOf[LocalDateTime]
    val inputObject = BoundReference(0, ObjectType(clazz), nullable = true)

    // call UTF8String.fromString(<instance>.toString) in Expression way
    val converter = StaticInvoke(
      classOf[UTF8String],
      StringType,
      "fromString",
      Invoke(inputObject, "toString", ObjectType(classOf[String])) :: Nil
    )

    val serializer = CreateNamedStruct(Literal("ts") :: converter :: Nil)

    val deserializer: Expression = StaticInvoke(
      clazz,
      ObjectType(clazz),
      "parse",
      Literal("ts") :: Nil)

    new ExpressionEncoder[LocalDateTime](
      serializer.dataType,
      flat = false,
      serializer.flatten,
      deserializer,
      classTag[LocalDateTime]
    )
  }
}