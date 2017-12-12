package com.company

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class RootRouter(spark: SparkSession)(implicit executionContext: ExecutionContext) extends LazyLogging {
  val executor = new SparkExecutor

  val routes: Route = extractUri { uri =>
    extractMethod { method =>
      logger.debug("{} {}", method.value, uri.toRelative.path)
      businessRoutes
    }
  }

  private val businessRoutes: Route =
    get {
      path("health") {
        complete(StatusCodes.OK)
      }
    } ~ post {
      path("spark" / "csv") {
        onComplete(executor.testCsv(spark)) {
          case Success(s) => complete(StatusCodes.OK, s)
          case Failure(e) =>
            logger.error("Error while processing CSV", e)
            complete(StatusCodes.InternalServerError, e.toString)
        }
      } ~ path("spark" / "time") {
        onComplete(executor.testLocalDateTime(spark)) {
          case Success(s) => complete(StatusCodes.OK, s)
          case Failure(e) =>
            logger.error("Error while processing LocalDateTime", e)
            complete(StatusCodes.InternalServerError, e.toString)
        }
      }
    }
}
