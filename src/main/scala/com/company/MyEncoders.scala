package com.company

import java.time.LocalDateTime

import com.company.model._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, CreateStruct, Expression, GetStructField, If, IsNull, Literal, Or, UpCast}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import reflect.runtime.universe._
import scala.reflect._

object MyEncoders {

  implicit def materialEncoder: ExpressionEncoder[Material] = {
    val clazz = classOf[Material]
    val inputObject = BoundReference(0, ObjectType(clazz), nullable = true)

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

  implicit def colorEncoder: ExpressionEncoder[Color] = {
    val clazz = classOf[Color]
    val inputObject = BoundReference(0, ObjectType(clazz), nullable = true)

    val converter = StaticInvoke(
      classOf[UTF8String],
      StringType,
      "fromString",
      Invoke(inputObject, "toString", ObjectType(classOf[String])) :: Nil
    )

    val serializer = CreateNamedStruct(Literal("value") :: converter :: Nil)

    val deserializer: Expression = StaticInvoke(
      Colors.getClass,
      ObjectType(clazz),
      "apply",
      Invoke(UpCast(GetColumnByOrdinal(0, StringType), StringType, "- root class: com.company.model.Color" :: Nil), "toString", ObjectType(classOf[String])) :: Nil)

    new ExpressionEncoder[Color](
      serializer.dataType,
      flat = true,
      serializer.flatten,
      deserializer,
      classTag[Color]
    )
  }

  def caseClass[T <: Product : TypeTag : ClassTag](encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[T] = {
    encoders.foreach(_.assertUnresolved())
    val fields = ScalaReflection.getConstructorParameters(typeTag[T].tpe).map(_._1).toList

    val schema = StructType(encoders.zipWithIndex.map {
      case (e, i) =>
        val (dataType, nullable) = if (e.flat) {
          e.schema.head.dataType -> e.schema.head.nullable
        } else {
          e.schema -> true
        }
        StructField(fields(i), dataType, nullable)
    })

    val cls = classTag[T].runtimeClass

    val serializer = encoders.zipWithIndex.map { case (enc, index) =>
      val originalInputObject = enc.serializer.head.collect { case b: BoundReference => b }.head
      val newInputObject = Invoke(
        BoundReference(0, ObjectType(cls), nullable = true),
        fields(index),
        originalInputObject.dataType)

      val newSerializer = enc.serializer.map(_.transformUp {
        case b: BoundReference if b == originalInputObject => newInputObject
      })

      if (enc.flat) {
        newSerializer.head
      } else {
        val struct = CreateStruct(newSerializer)
        val nullCheck = Or(
          IsNull(newInputObject),
          Invoke(Literal.fromObject(None), "equals", BooleanType, newInputObject :: Nil))
        If(nullCheck, Literal.create(null, struct.dataType), struct)
      }
    }

    val childrenDeserializers = encoders.zipWithIndex.map { case (enc, index) =>
      if (enc.flat) {
        enc.deserializer.transform {
          case g: GetColumnByOrdinal => g.copy(ordinal = index)
        }
      } else {
        val input = GetColumnByOrdinal(index, enc.schema)
        val deserialized = enc.deserializer.transformUp {
          case UnresolvedAttribute(nameParts) =>
            assert(nameParts.length == 1)
            UnresolvedExtractValue(input, Literal(nameParts.head))
          case GetColumnByOrdinal(ordinal, _) => GetStructField(input, ordinal)
        }
        If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
      }
    }

    val deserializer =
      NewInstance(cls, childrenDeserializers, ObjectType(cls), propagateNull = false)

    new ExpressionEncoder[T](
      schema,
      flat = false,
      serializer,
      deserializer,
      ClassTag(cls))
  }

  implicit def scalaLocalDateTime: ExpressionEncoder[LocalDateTime] = {
    val clazz = classOf[LocalDateTime]
    val inputObject = BoundReference(0, ObjectType(clazz), nullable = true)

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