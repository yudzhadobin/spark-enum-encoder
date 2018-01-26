package com.company.model

case class BodyWorkpiece
(
  id: Int,
  width: Double,
  materialId: Int,
  colorId: Int
)

case class ColorWorkpiece
(
  id: Int,
  color: String
)

case class MaterialWorkpiece
(
  id: Int,
  material: String
)

case class Second
(
  material: Material,
  price: Int
)