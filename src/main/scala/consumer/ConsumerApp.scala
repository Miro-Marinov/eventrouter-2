package consumer

import akka.actor.ActorSystem
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import kafka.KafkaConfig

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContextExecutor, Future}

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
  def processSync(msgs: ArrayBuffer[String]): Unit =
    msgs.foreach { msg => println(msg) }

  def routeFromSource(topic: String) =
    Consumer.committableSource(kafkaConfig.consumerSettings, Subscriptions.topics(topic))
      .map(msg => (msg.committableOffset, msg.record.value()))
      // batch if consuming from Kafka is too fast
      .batch(max = 20, { case (offset, msg) => (CommittableOffsetBatch.empty.updated(offset), ArrayBuffer[String](msg)) }) { (acc, tuple) => (acc._1.updated(tuple._1), acc._2 :+ tuple._2) }
      //Take the first element of the tuple (set of commit numbers) to add to kafka commit log and then return the collection of grouped messages
      .map { case (offsets, msgs) => processSync(msgs); (offsets, msgs) }
      .mapAsync(4)(tuple => commitOffsetsToKafka[String](tuple))
      .map(msgGroup => msgGroup._2)
      .runWith(Sink.ignore)

  routeFromSource("firstTopic")

  def commitOffsetsToKafka[msgType](tuple: (ConsumerMessage.CommittableOffsetBatch, ArrayBuffer[msgType])) = Future {
    (tuple._1.commitScaladsl(), tuple._2)
  }

}
