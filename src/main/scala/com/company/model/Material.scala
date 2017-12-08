package com.company.model

sealed trait Material {
}

object Materials {
  object Wood extends Material
  object Metal extends Material
  object Glass extends Material
}
