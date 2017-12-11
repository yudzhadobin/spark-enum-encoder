package com.company.model

import java.time.LocalDateTime

case class Body
(
  id: Int,
  width: Double,
  material: Material,
  color: String,
  d: LocalDateTime
)
