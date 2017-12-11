package com.company.model

import org.apache.spark.sql.catalyst.DefinedByConstructorParams

sealed trait Material extends DefinedByConstructorParams{
  val name: String

  override def toString: String = name
}

object Materials {
  object Wood extends Material {override val name: String = "wood"}
  object Metal extends Material {override val name: String = "metal"}
  object Glass extends Material {override val name: String = "glass"}

  def valueOf(s: String): Material = s match {
    case "wood" => Wood
    case "metal" => Metal
    case "glass" => Glass
  }
}
