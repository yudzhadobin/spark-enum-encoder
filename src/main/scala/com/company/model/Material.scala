package com.company.model

sealed trait Material {
  val name: String
  override def toString: String = name
}

object Materials {
  object Wood extends Material {override val name: String = "wood"}
  object Metal extends Material {override val name: String = "metal"}
  object Glass extends Material {override val name: String = "glass"}

  def apply(s: String): Material = s match {
    case "wood" => Wood
    case "metal" => Metal
    case "glass" => Glass
    case null => null
    case other => throw new IllegalArgumentException(s"'$other' not a Material")
  }
}
