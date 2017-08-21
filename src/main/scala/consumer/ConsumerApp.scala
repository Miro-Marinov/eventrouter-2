package consumer

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.model._
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import kafka.KafkaConfig

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

/**
  * Created by mirob on 8/21/2017.
  */
object ConsumerApp extends App {

  // Note that the default support renders the Source as JSON Array
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()
  implicit val system = ActorSystem()
  lazy val kafkaConfig = KafkaConfig(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  // Process before committing to kafka. TODO: Think how to implement some retry logic
  def processSync(msgs: Seq[String]): Unit =
    msgs.foreach { msg => println(msg) }

  def routeFromSource(topic: String) =
    Consumer.committableSource(kafkaConfig.consumerSettings, Subscriptions.topics(topic))
      .map(msg => (msg.committableOffset, msg.record.value()))
      // batch if consuming from Kafka is too fast
      .batch(max = 20, {
      case (offset, msg) => (CommittableOffsetBatch.empty.updated(offset), Seq[String](msg))
    }) { (acc, tuple) => (acc._1.updated(tuple._1), acc._2 :+ tuple._2) }
/*      .groupedWithin(10, 10 seconds)
      .map(seqPairs => seqPairs.foldLeft((CommittableOffsetBatch.empty, Seq.empty[String]))((acc, pair) => {
        (acc._1.updated(pair._1), acc._2 :+ pair._2)
      }))*/
      //Take the first element of the tuple (set of commit numbers) to add to kafka commit log and then return the collection of grouped messages
      .map { case (offsets, msgs) => processSync(msgs); (offsets, msgs) }
      .map { case (offsets, msgs) => {
        val msgsArray = msgs.toString.replace("List(", "[").replace(")", "]")
        val request = HttpRequest(uri = Uri.from(scheme = "http", host = "localhost", port = 8000, path = "/routed"))
          .withMethod(HttpMethods.POST)
          .withEntity(HttpEntity.apply(msgsArray).withContentType(ContentTypes.`application/json`))
        println(s"Sending to uri ${request.uri.toString} msg: ${msgsArray}")
        println(request)
        (request, offsets)
      }
      }
      .via(httpRoute)
      .map { case (tryRes, offsets) =>
        println(s"Tried response: $tryRes")
        (tryRes, offsets)
      }
      //        .recoverWithRetries(5)
      .mapAsync(4)(tuple => commitOffsetsToKafka[Try[HttpResponse]](tuple))
      .map(msgGroup => msgGroup._1)
      .runWith(Sink.ignore)

  def httpRoute = Http().cachedHostConnectionPool[ConsumerMessage.CommittableOffsetBatch]("localhost", 8000)

  routeFromSource("firstTopic")

  def commitOffsetsToKafka[T](tuple: (T, ConsumerMessage.CommittableOffsetBatch)) = Future {
    (tuple._1, tuple._2.commitScaladsl())
  }

}
