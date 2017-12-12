package com.company

import java.time.LocalDateTime

import com.company.model.{Material, Materials}
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, Literal, UpCast}
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

    val serializer = CreateNamedStruct(Literal("value") :: converter :: Nil)

    val deserializer: Expression = StaticInvoke(
      Materials.getClass,
      ObjectType(clazz),
      "apply",
      Invoke(UpCast(GetColumnByOrdinal(0, StringType), StringType, "- root class: com.company.model.Material" :: Nil), "toString", ObjectType(classOf[String])) :: Nil)

    new ExpressionEncoder[Material](
      serializer.dataType,
      flat = true,
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

    val serializer = CreateNamedStruct(Literal("value") :: converter :: Nil)

    val deserializer: Expression = StaticInvoke(
      clazz,
      ObjectType(clazz),
      "parse",
      Invoke(UpCast(GetColumnByOrdinal(0, StringType), StringType, "- root class: java.time.LocalDateTime" :: Nil), "toString", ObjectType(classOf[String])) :: Nil)

    new ExpressionEncoder[LocalDateTime](
      serializer.dataType,
      flat = true,
      serializer.flatten,
      deserializer,
      classTag[LocalDateTime]
    )
  }
}