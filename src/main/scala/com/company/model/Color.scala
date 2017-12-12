package com.company.model

sealed trait Color {
  val name: String
  override def toString: String = name
}

object Colors {
  object Red extends Color {override val name: String = "red"}
  object Green extends Color {override val name: String = "green"}
  object Blue extends Color {override val name: String = "blue"}

  def apply(s: String): Color = s match {
    case "red" => Red
    case "green" => Green
    case "blue" => Blue
    case null => null
    case other => throw new IllegalArgumentException(s"'$other' not a Color")
  }
}