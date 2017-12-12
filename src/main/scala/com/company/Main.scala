package com.company
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Main extends LazyLogging {
  def main(args: Array[String]) {
    implicit val actorSystem: ActorSystem = ActorSystem()
    implicit val executor: ExecutionContext = actorSystem.dispatcher
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    try {
      val config = ConfigFactory.load()
      val spark = SparkSession.builder
        .appName("spark-encoding")
        .config("spark.master", "local[2]")
        .getOrCreate()

      val router = new RootRouter(spark)

      Http().bindAndHandle(router.routes, config.getString("http.host"), config.getInt("http.port")).onComplete {
        case Success(b) => logger.info("Bound on {}", b.localAddress)
        case Failure(e) => throw e
      }
    } catch {
      case e: Throwable =>
        logger.error("Error while initializing app, shutdown", e)
        actorSystem.terminate().onComplete {
          case Success(t) => logger.info("Terminated {}", t)
          case Failure(err) =>
            logger.error("Termination failed with error", err)
            sys.exit(100500)
        }
    }

  }
}