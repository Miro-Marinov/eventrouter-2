package kafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Producer, _}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import json.EventJsonProtocol
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import spray.json.JsValue

case class KafkaConfig(system: ActorSystem) extends SprayJsonSupport with EventJsonProtocol {

  import system.dispatcher

  val bootstrapServers: String = system.settings.config.getString("akka.kafka.bootstrap-servers")
  // See application.conf: akka.kafka.consumer
  val consumerSettings: ConsumerSettings[Array[Byte], String] = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("grp-1")
    .withClientId("client-1")

  val producerSettings: ProducerSettings[Array[Byte], String] = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def toKafkaSink: Sink[JsValue, NotUsed] =
    Flow[JsValue]
      .map { msg =>
        val topic = getTopic(msg).getOrElse("deadLetter")
        println(msg.toString())
        println(topic)
        // For some reason it only works if I hardcore the topic here
        new ProducerRecord[Array[Byte], String]("firstTopic", msg.toString())
      }
      .to(Producer.plainSink(producerSettings))

  // get the header.topic field
  def getTopic(msg: JsValue) = {
    msg.asJsObject.fields.get("header").flatMap(_.asJsObject().fields.get("topic").map(_.toString))
  }

  def firstSource(implicit mat: Materializer): Source[CommittableMessage[Array[Byte], JsValue], Control] =
    Consumer.committableSource(consumerSettings, Subscriptions.topics("firstTopic"))
      .mapAsync(5)(c => Unmarshal(c.record.value()).to[JsValue].map(s =>
        c.copy(record = new ConsumerRecord("firstTopic", c.record.partition(), c.record.offset(), c.record.key(), s))))


}
