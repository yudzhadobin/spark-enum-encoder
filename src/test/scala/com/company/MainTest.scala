package com.company

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{Matchers, WordSpec}

class MainTest extends WordSpec with Matchers with LazyLogging {
  "bootstrap case should pass" should {
    logger.trace("Case should pass")
  }

//  "bootstrap case should fail" in {
//    logger.info("Case should fail")
//    true should be (false)
//  }
}
