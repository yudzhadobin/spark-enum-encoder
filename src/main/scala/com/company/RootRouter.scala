package com.company

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import com.company.model.Body
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class RootRouter(spark: SparkSession) extends LazyLogging {
  val routes: Route = extractUri { uri =>
    extractMethod { method =>
      logger.debug("{} {}", method.value, uri.toRelative.path)
      businessRoutes
    }
  }

  private val businessRoutes: Route =
    get {
      path("/health") {
        complete(StatusCodes.OK)
      }
    } ~ post {
      import spark.implicits._

      path("spark" / "csv") {
        val schema = StructType
        val data = spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("src/main/resources/sample.csv")
          .as[Body]

        val result = data.filter(d => d.height > 1)

        complete(StatusCodes.OK, s"total: ${result.count()}")
      }
    }
}
