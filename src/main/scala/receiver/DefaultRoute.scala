package receiver

import akka.actor.ActorSystem
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.ActorMaterializer
import json.EventJsonProtocol
import kafka.KafkaConfig
import spray.json.JsValue

import scala.concurrent.ExecutionContextExecutor


/**
  * Created by mirob on 8/19/2017.
  */
trait DefaultRoute extends Directives with EventJsonProtocol {

  // Note that the default support renders the Source as JSON Array
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()
  implicit val system = ActorSystem()
  lazy val kafkaConfig = KafkaConfig(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  // @formatter:off
  val defaultRoute: Route =
    get {
      path("status") {
        complete("OK")
      }
    } ~
    post {
      entity(asSourceOf[JsValue]) { jsonSource =>
          jsonSource.runWith(kafkaConfig.toKafkaSink)
          complete("OK")
        }
    }
  // @formatter:on
}
