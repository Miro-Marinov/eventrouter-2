package receiver

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future

/**
  * Created by mirob on 8/19/2017.
  */
object ReceiverApp extends App with Routes {
  val host = "localhost"
  val port = 8000
  val binding: Future[ServerBinding] = Http().bindAndHandle(routes, host, port)
  println(s"Bound to: $host:$port")
  println(s"Kafka servers: ${kafkaConfig.bootstrapServers}")

/*  val done = Source(1 to 100)
    .map(_.toString)
    .map { elem =>
      new ProducerRecord[Array[Byte], String]("firstTopic", elem)
    }
    .runWith(Producer.plainSink(kafkaConfig.producerSettings))*/
}
